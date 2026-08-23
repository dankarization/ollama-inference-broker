# Локальный брокер инференса

Это приватный control plane для одного GPU-хоста с Ollama. Брокер предотвращает
конкуренцию локальных задач за VRAM и предоставляет строгую очередь с
приоритетами. Это изолированный MVP: он не меняет текущих callers, трафик,
маршрутизацию, production-конфигурацию или порядок source roots.

## Назначение

Ollama хранит веса моделей, но каждый параллельный запрос использует собственный
KV cache и буферы генерации либо изображений. При больших контекстах или смене
модели это может исчерпать VRAM. Брокер является единой точкой admission для
локального инференса: callers ставят задания в очередь, а не вызывают Ollama
напрямую.

```text
Постоянный хост OpenClaw: broker + SQLite WAL      MAIN-PC: executor Ollama
Задания OpenClaw / M101 ──> очередь, policy, API ──> Ollama ──> один GPU / VRAM
                              resource: mainpc-gpu       ^
                                                   WOL при dispatch задания
```

Cloud-routed нагрузки, CPU-задачи и существующие GPU-workers Whisper остаются
вне этого пути. Брокер запускается вне MAIN-PC, поэтому его очередь переживает
WOL cycle; MAIN-PC не хранит состояние очереди.

## API MVP

Запускайте только в лабораторной или staging-среде:

```bash
MAINPC_MAC=2c:f0:5d:74:6a:44 python -m broker
```

По умолчанию сервер слушает localhost и не изменяет конфигурации OpenClaw,
Ollama, Shutterstock, M101 или MAIN-PC.

### Локальный user-service: admission без dispatch

В `systemd/ollama-inference-broker.service` намеренно зафиксированы loopback
`127.0.0.1:8088`, локальный SQLite в `~/.local/state/ollama-inference-broker`
и `BROKER_DISPATCH_ENABLED=false`. В таком режиме service переживает сбой,
принимает и устойчиво сохраняет jobs, но не запускает dispatcher: WOL, `/api/ps`
и любой запрос к MAIN-PC/Ollama невозможны. Это безопасный deployment state до
отдельного разрешения на canary dispatch.

```bash
install -D -m 0644 systemd/ollama-inference-broker.service \
  ~/.config/systemd/user/ollama-inference-broker.service
systemctl --user daemon-reload
systemctl --user enable --now ollama-inference-broker.service
curl --fail http://127.0.0.1:8088/healthz
```

Rollback: `systemctl --user disable --now ollama-inference-broker.service`.
Удаление SQLite state не требуется для остановки и должно выполняться только
после отдельной проверки queued jobs.

- `POST /v1/jobs` принимает `{ "profile":"interactive", "kind":"chat|generate", "payload":{...} }` и возвращает сохранённое задание (`202`).
- `GET /v1/jobs/{id}`, `POST /v1/jobs/{id}/cancel` и `GET /v1/metrics` дают доступ к жизненному циклу и данным MAIN-PC `/api/ps`.
- `GET /healthz` проверяет только локальное состояние broker: очередь, активную lease и timestamp. Он не отправляет WOL и не обращается к MAIN-PC/Ollama.
- `/api/chat` и `/api/generate` реализуют локальный compatibility contract:
  обязателен server-side `profile`, а `stream=true` возвращает NDJSON admission
  frame. Endpoint только ставит job в очередь и не dispatch-ит его; реальный
  model output не обещается до отдельной integration/canary фазы.

Сервер сам выбирает профиль, модель, контекст, лимит вывода и keepalive.
Переданные caller значения `model`, `num_ctx`, `num_predict` и `keep_alive` не
могут повысить эти лимиты. SQLite использует WAL. Dispatch выполняется строго
по приоритету, затем FIFO; старение очереди намеренно не применяется.

## Действующая политика приоритетов

Меньшее число означает более высокий приоритет.

| Источник / ключ конфигурации | Фиксированный приоритет |
| --- | ---: |
| Интерактивная сессия OpenClaw (`interactive`) | 1 |
| OpenClaw cron (`cron`) | 2 |
| Локальное Shutterstock video (`shutterstock-video`) | 5 |
| Olya (`olya`) | 8 |

Shutterstock photo остаётся cloud workload через OmniRoute и не имеет профиля
broker: оно не ставит задание в эту очередь, не отправляет WOL и не занимает
GPU MAIN-PC. `shutterstock-video` — отдельный локальный workload на
`nemotron3:33b`; только он получает приоритет 5. Ключ `olya` — только имя
источника, а не интеграция. Эти четыре значения
принадлежат broker: caller может не передавать `priority` либо повторить
фиксированное значение, но не может его переопределить. Остальные источники
обязаны передать целый `priority` от 1 (максимальный) до 10 (минимальный),
например `{ "profile":"batch-video", "source":"maintenance", "priority":7 }`.

При смене модели broker будит MAIN-PC, читает `/api/ps`, выгружает несовместимую
модель, запрашивает и проверяет готовность целевой модели и только затем
запускает задание. Просроченная running lease возвращается в очередь при
перезапуске broker.

Если `BROKER_DISPATCH_ENABLED=true`, обязательно задаётся непустой
`BROKER_DISPATCH_SOURCES` с точными именами источников через запятую. Dispatcher
берёт lease только у заданий из этого allowlist; все остальные задания остаются
durable `queued`. Это позволяет включить обратимый pilot без миграции либо
активации других callers.

## Проверка и разработка

Безопасная canary-проверка использует mock или staging задания `interactive`,
`cron`, `shutterstock`, `olya` и dynamic priority. Следует проверить один
удалённый запрос за раз, порядок priority/FIFO, WOL/readiness, unload перед
сменой модели, восстановление просроченной lease и отмену queued задания.
Ни один live caller не мигрируется до успешной проверки; откат — остановить
broker без изменения routes.

Перед live canary оператор обязан проверить, что на MAIN-PC уже установлены
все модели server-side local-GPU profiles. Для `shutterstock-video` нужна
`nemotron3:33b`; cloud photo через OmniRoute этой проверки не требует и в
broker не подключается. Если целевой модели нет в `/api/tags`, canary
прекращается до dispatch, WOL, unload или попытки pull модели.

```bash
python -m unittest discover -s tests -v
```

Тесты используют адаптеры-заглушки и не отправляют WOL либо запросы Ollama.

## Границы MVP

Broker владеет admission, порядком, residency моделей, request profiles и
операционной телеметрией. Ollama остаётся inference runtime. Проект не заменяет
cloud routing, task orchestration или бизнес-логику callers.

Подробный поэтапный план — в [ROADMAP.md](ROADMAP.md).

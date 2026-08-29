# Дорожная карта

Обозначения: `[x]` — реализовано и проверено изолированными тестами; `[ ]` —
ещё не выполнено либо требует отдельного разрешения и внешней проверки.

## Фаза 0 — контракт и инвентаризация

- [x] Определены API broker, workload profiles, source weights, отмена и
  локальная SQLite WAL очередь.
- [ ] Инвентаризация всех прямых callers локального Ollama, владельцев их
  миграции и границ local/cloud routing. Это требует отдельного scope и не
  выполняется изолированным MVP.

## Фаза 1 — MVP безопасного последовательного admission

- [x] Один процесс broker с durable queue, weighted source scheduling и FIFO
  внутри выбранного source.
- [x] Одна активная локальная нагрузка, SQLite leases, отмена queued заданий и
  requeue просроченной lease после restart.
- [x] Server-owned profile limits, readiness check, controlled unload/switch и
  ограниченный keepalive в коде broker.
- [x] Отделён local-GPU профиль `shutterstock-video` (`nemotron3:33b`,
  приоритет 3) от cloud Shutterstock photo: фото не имеет broker profile и не
  может получить MAIN-PC GPU.
- [x] Локальный `GET /healthz`: состояние очереди и активной lease без WOL,
  запроса к MAIN-PC или обращения к Ollama.
- [x] Добавлен отдельный Phase-2 text endpoint `syncopia-telegram-memory`:
  pinned qwen38, 64k context, `think=low`, tools disabled и production policy
  weight `4`; неизвестные ключи source policy отклоняются fail-closed.
- [x] Развёрнут loopback-only user-service с durable SQLite и явным
  admission-only guard (`BROKER_DISPATCH_ENABLED=false`). Проверены `/healthz`
  и приём/отмена synthetic job: dispatch, WOL и MAIN-PC/Ollama не вызывались.
- [ ] Canary с mock/staging заданиями и подтверждением WOL/readiness,
  unload-before-switch и отсутствия overlap на MAIN-PC. Сейчас заблокировано:
  новый профиль `shutterstock-video` использует уже подтверждённый
  `nemotron3:33b`, но сам live canary по-прежнему требует отдельного
  production authorization. До него не делаются WOL, unload либо caller migration.

MVP остаётся control plane на постоянном хосте OpenClaw для ресурса
`mainpc-gpu`; MAIN-PC — только executor. Эта фаза не меняет callers, трафик,
маршрутизацию, production configuration, сервисы, расписания или source-root
order.

## Фаза 2 — интеграции

- [x] Реализован и изолированно проверен Ollama-compatible
  admission/streaming contract: `/api/chat` и `/api/generate` требуют
  server-side profile и возвращают NDJSON admission frame; serializer также
  определяет terminal-state frames для уже сохранённого job. Endpoint сам не
  ждёт и не dispatch-ит job.
- [ ] Затем мигрировать один явно разрешённый canary caller.
- [ ] Позднее перевести OpenClaw, Hermes и локальные pipelines; не
  перенаправлять процесс только из-за места его запуска.

Интеграция, migration callers, traffic routes, real model output и canary этой
фазы намеренно не начаты и остаются вне текущего scope.

## Фаза 3 — наблюдаемость и операции

- [x] Добавлены durable audit events, история attempts/retry/requeue,
  payload-free correlation lookup, per-source latency/throughput/error metrics,
  scheduler decision context и fairness windows. Действующий scheduler не
  изменён.
- [ ] Добавить health checks за пределами локального `/healthz`, dashboards и
  alerts поверх накопленной истории.
- [ ] Публиковать данные MAIN-PC о VRAM и loaded models только при отдельно
  разрешённом внешнем обращении.

## Фаза 4 — контролируемый rollout и настройка

- [ ] Rollout по классам callers, настройка profile limits/keepalive по данным
  очереди и VRAM и проверенный rollback к serialized lock-and-queue.

Эта фаза требует отдельного production authorization.

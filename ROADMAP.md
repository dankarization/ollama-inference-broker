# Дорожная карта

Обозначения: `[x]` — реализовано и проверено изолированными тестами; `[ ]` —
ещё не выполнено либо требует отдельного разрешения и внешней проверки.

## Фаза 0 — контракт и инвентаризация

- [x] Определены API broker, workload profiles, server-owned priority classes,
  отмена и локальная SQLite WAL очередь.
- [x] Зафиксирована политика: `interactive` — 1, `cron` — 2,
  `shutterstock-video` — 5, `olya` — 8. Меньшее число означает более высокий
  приоритет; Shutterstock photo остаётся cloud/OmniRoute вне broker, а другие
  источники передают целое число от 1 до 10.
- [ ] Инвентаризация всех прямых callers локального Ollama, владельцев их
  миграции и границ local/cloud routing. Это требует отдельного scope и не
  выполняется изолированным MVP.

## Фаза 1 — MVP безопасного последовательного admission

- [x] Один процесс broker с durable queue, strict priority и FIFO при равном
  приоритете.
- [x] Одна активная локальная нагрузка, SQLite leases, отмена queued заданий и
  requeue просроченной lease после restart.
- [x] Server-owned profile limits, readiness check, controlled unload/switch и
  ограниченный keepalive в коде broker.
- [x] Отделён local-GPU профиль `shutterstock-video` (`nemotron3:33b`,
  приоритет 5) от cloud Shutterstock photo: фото не имеет broker profile и не
  может получить MAIN-PC GPU.
- [x] Локальный `GET /healthz`: состояние очереди и активной lease без WOL,
  запроса к MAIN-PC или обращения к Ollama.
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

- [ ] Добавить health checks за пределами локального `/healthz`, structured
  logs, dashboards и alerts.
- [ ] Публиковать данные MAIN-PC о VRAM и loaded models только при отдельно
  разрешённом внешнем обращении.

## Фаза 4 — контролируемый rollout и настройка

- [ ] Rollout по классам callers, настройка profile limits/keepalive по данным
  очереди и VRAM и проверенный rollback к serialized lock-and-queue.

Эта фаза требует отдельного production authorization.

# Аналитика очереди и scheduler

Этот слой нужен для сравнения scheduler algorithms на исторических данных. Он
не меняет действующий выбор: без runtime policy остаётся global FIFO,
с policy — model-aware scheduler с 60-минутными weighted time batches,
FIFO head внутри source и non-preemptive границами batch.

## Что сохраняется

SQLite содержит две append-oriented таблицы:

- `audit_events` — admission, scheduler selection, переходы
  `queued → running → terminal`, lease renew/expire, requeue, retry и cancel;
- `job_attempts` — отдельная строка каждой попытки с временем постановки,
  выбора, запуска, lease, завершения, outcome и scheduler context.

В `jobs` добавлены `source_item_id`, `external_id`, `queued_at` и счётчики
attempt/retry/requeue. Это additive migration: старые строки получают
`queued_at=created`, остальные значения nullable/defaulted. Payload и result
не копируются в audit/attempt tables и не попадают в structured audit logs.
Correlation identifiers ограничены строкой до 256 символов; передавать в них
секреты или пользовательский контент нельзя.

История начинается с rollout этой версии. Для старых jobs не создаются
вымышленные scheduler decisions/attempts; их текущее состояние видно в
`current`, но historical window counters учитывают только новые audit events.

Каждый `scheduler.selected` сохраняет выбранный source, mode и причину,
реально eligible sources и активные weights из последней hot policy. Поэтому
atomic reload policy не обнуляет историю и позволяет восстановить условия
каждого решения.

## Read-only API

```bash
curl -sS 'http://127.0.0.1:8088/v1/analytics?window=300,1800,10800,86400'
curl -sS 'http://127.0.0.1:8088/v1/audit-events?source=olya-vision&limit=100'
curl -sS 'http://127.0.0.1:8088/v1/jobs/JOB_ID/attempts'
curl -sS 'http://127.0.0.1:8088/v1/correlations?source=olya-vision&external_id=ITEM_ID'
```

`window` задаётся в секундах, максимум один год. History limit ограничен 1000.
Correlation lookup возвращает только job/source/profile/state/timestamps и
идентификаторы, без payload/result. `GET /v1/jobs/{id}` оставлен без изменений
для legacy jobs. Producer-storage jobs и их audit metadata видны через эти
read endpoints только с owner-only storage bearer token; unauthenticated
history/audit/correlation ответы их исключают через persisted visibility marker
и partial indexes, а legacy analytics остаётся доступной как прежде.

Новые callers могут передавать в `POST /v1/jobs`:

```json
{
  "profile": "olya",
  "kind": "generate",
  "payload": {},
  "source_item_id": "source-local-id",
  "external_id": "upstream-id"
}
```

Повтор failed/cancelled job доступен явно через
`POST /v1/jobs/{id}/retry`; для producer-storage job endpoint требует тот же
owner-only bearer token, что и status/receipt. Lease renewal подготовлен как
broker method для будущего внешнего executor contract; текущий синхронный
dispatcher его не вызывает и работает как прежде.

Bulk cancel/retry сохраняет legacy behavior, но если выбранный набор содержит
producer-storage job, весь mutation требует storage bearer token и до проверки
не изменяет ни одной строки.

## Метрики

Для каждого source snapshot показывает:

- текущие queue depth/running и cumulative terminal counts;
- admitted/dispatched/completed/failed/cancelled и retry/requeue;
- queue wait: `attempt.started - attempt.queued_at`;
- inference latency: `attempt.finished - attempt.started`;
- end-to-end latency: `job.finished - job.created`;
- throughput completed/hour и success rate;
- avg/p50/p95/max для трёх latency classes.

Scheduler window сохраняет actual selection count/share и mode. Для
`time_batch` fairness нельзя выводить из числа jobs: scheduler context каждого
`scheduler.selected` содержит `horizon_seconds`, `time_budget_seconds` и
накопленное время lane на момент выбора. Цель — доля **execution time**,
пропорциональная прямому Weight среди ready lanes. Поэтому при постоянно
заполненных weights `10` и `8` один 3600-секундный horizon имеет targets
2000s и 1600s; последний non-preemptive job может дать документированный
overrun. Disabled, пустые и rate-limited sources не делают GPU idle и не
считаются нарушением доли.

## Что сравнивать позже

Алгоритм нельзя менять до накопления baseline и отдельного rollout. Затем на
одинаковом replay workload безопасно сравниваются:

- time-batch scheduler — текущий baseline: 60-минутная прямая доля времени,
  model affinity и FIFO внутри source;
- deficit round robin — лучше учитывает разную стоимость job, если появится
  надёжная оценка cost;
- weighted fair queue — полезен при нескольких непрерывно загруженных sources,
  но требует виртуального времени/cost;
- aging — уменьшает starvation, но должен сохранять FIFO-инварианты source.

Основные критерии: p95 wait/e2e по source, throughput, success/retry/requeue,
starvation (максимальный wait), actual-vs-expected share и смены модели. Нельзя
оценивать fairness отдельно от eligibility и нельзя оптимизировать throughput
ценой terminal failures либо p95 интерактивных задач.

Перед production rollout: backup SQLite, прогон миграции на копии актуальной
БД, проверка новых GET endpoints, затем restart service в отдельном
авторизованном окне. Откат к старому бинарнику безопасен: он игнорирует новые
таблицы/колонки; удалять их при rollback не нужно.

## Почасовой Telegram-статус очереди

`ollama-inference-broker-report.timer` запускает отдельный one-shot процесс в
`*:00:30 Asia/Tbilisi`. Каждый запуск формирует отчёт **строго за предыдущий
закрытый wall-clock час** `[HH:00, HH+1:00)` в `Asia/Tbilisi` и отправляет его,
включая нулевые часы. Текст имеет короткий фиксированный русский HTML-шаблон:
он читает `jobs`, `audit_events` и текущий `sources.json` непосредственно.
`<tg-time>` даёт нативные активные Telegram даты/время; динамические
идентификаторы HTML-экранируются и ограничены тремя компактными значениями.

В отчёте отображается effective **weight** каждого source: это единственный
операторский scheduling-параметр. Runtime policy использует weights для
weighted round-robin между sources, а внутри выбранного source сохраняется
FIFO.

Размеры файлов и queue delta не выводятся: в broker SQLite нет надёжного
поля размера и почасового baseline snapshot. Health состоит только из
`systemctl is-active`, локального `/healthz` и Ollama `/api/ps`; эти probes не
посылают WOL и не запускают model inference.

Доставка следует используемой Airfare monitor схеме Telegram Bot API:
`parse_mode=HTML`, `message_thread_id` и `disable_web_page_preview=true`.
Учётная запись читается из локальной OpenClaw configuration внутри процесса;
credential не попадает в unit, argv приложения, SQLite или логи. Target,
account и thread задаются только в reporter unit. `status_report_outbox` —
additive durable outbox: подтверждённый Telegram `message_id` делает закрытый
час идемпотентным. `status_report_manual_outbox` отделяет явно авторизованный
ручной resend (`--manual-key`) от hourly dedupe. Pre-flight failure можно
безопасно повторить. Ошибка сети после начала HTTP-запроса помечается
`uncertain` и автоматически **не** повторяется, чтобы не создать второй пост
без idempotency key Telegram Bot API.

Перед первым rollout обязателен согласованный SQLite backup, потому что
`status_report_outbox` создаётся в production БД. Проверка:

```bash
systemctl --user enable --now ollama-inference-broker-report.timer
systemctl --user start ollama-inference-broker-report.service
systemctl --user status ollama-inference-broker-report.service --no-pager
systemctl --user list-timers ollama-inference-broker-report.timer --all
```

В reporting path нет импорта `OllamaHTTP`, compatibility endpoints или
`/api/generate`; regression test закрепляет это ограничение.

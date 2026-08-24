# Аналитика очереди и scheduler

Этот слой нужен для сравнения scheduler algorithms на исторических данных. Он
не меняет действующий выбор: без runtime policy остаётся strict priority/FIFO,
с policy — существующий weighted round-robin.

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

Каждый `scheduler.selected` сохраняет выбранный source/priority, mode и причину,
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
ради обратной совместимости.

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
`POST /v1/jobs/{id}/retry`. Lease renewal подготовлен как broker method для
будущего внешнего executor contract; текущий синхронный dispatcher его не
вызывает и работает как прежде.

## Метрики

Для каждого source snapshot показывает:

- текущие queue depth/running и cumulative terminal counts;
- admitted/dispatched/completed/failed/cancelled и retry/requeue;
- queue wait: `attempt.started - attempt.queued_at`;
- inference latency: `attempt.finished - attempt.started`;
- end-to-end latency: `job.finished - job.created`;
- throughput completed/hour и success rate;
- avg/p50/p95/max для трёх latency classes.

Scheduler window показывает actual selection count/share и expected share.
Expected share рассчитывается на каждом решении только среди sources, которые
тогда действительно были eligible. Это не штрафует scheduler за пустую очередь,
rate limit или disabled source. `fairness_ratio=1` и малый
`absolute_share_error` означают близость actual share к доступной weighted цели.

Пример после 17 dispatch opportunities при постоянно заполненных очередях и
weights `olya-vision=8`, `olya-decision=6`, `shutterstock-video=3`:

```json
{
  "selections": 17,
  "sources": {
    "olya-vision": {
      "selected": 8,
      "actual_share": 0.470588,
      "expected_share": 0.470588,
      "fairness_ratio": 1.0,
      "absolute_share_error": 0.0
    },
    "olya-decision": {
      "selected": 6,
      "actual_share": 0.352941,
      "expected_share": 0.352941,
      "fairness_ratio": 1.0,
      "absolute_share_error": 0.0
    },
    "shutterstock-video": {
      "selected": 3,
      "actual_share": 0.176471,
      "expected_share": 0.176471,
      "fairness_ratio": 1.0,
      "absolute_share_error": 0.0
    }
  }
}
```

На коротком окне дискретность закономерно даёт отклонение. Для оценки нужны
одновременно 5 минут, 30 минут, 3 часа и 24 часа.

## Что сравнивать позже

Алгоритм нельзя менять до накопления baseline и отдельного rollout. Затем на
одинаковом replay workload безопасно сравниваются:

- weighted round robin — текущий baseline, прост и детерминирован;
- deficit round robin — лучше учитывает разную стоимость job, если появится
  надёжная оценка cost;
- weighted fair queue — полезен при нескольких непрерывно загруженных sources,
  но требует виртуального времени/cost;
- aging поверх bounded priority — уменьшает starvation, но должен сохранять
  hard priority constraints.

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

В отчёте есть только одна операторская шкала: **приоритет 1–10** (1 выше, 10
ниже): Shutterstock 3, Olya Vision 8, Olya Decision 6. Это фактический
server-owned `job.priority`, а не число из source policy. Внутри broker
runtime policy всё ещё хранит отдельные private weights для weighted
round-robin между sources; priority упорядочивает jobs *внутри* выбранного
source. Репорт их не печатает, а это изменение не меняет текущие weights,
allocation или throughput.

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

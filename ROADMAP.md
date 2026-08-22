# Дорожная карта

## Фаза 0 — контракт и инвентаризация

Определить API broker, идентичности callers, workload profiles, классы
приоритетов, отмену и инвентарь моделей. Зафиксировать всех прямых callers
локального Ollama и поддерживаемые сигналы здоровья MAIN-PC.

Действующая фиксированная политика: `interactive` — 1, `cron` — 2,
`shutterstock` — 5 и `olya` — 8. Меньшее число означает более высокий
приоритет. Другие источники передают целый приоритет от 1 до 10.

Переход возможен, когда у каждого предполагаемого caller есть владелец миграции
и нет неясности между local и cloud routing.

## Фаза 1 — MVP безопасного последовательного admission

Реализовать один процесс broker с устойчивой очередью и одной активной локальной
нагрузкой. Нужны strict priority, затем FIFO при равенстве, exclusive access к
Ollama, readiness checks целевой модели, управляемые unload/switch, profile
limits контекста и вывода и ограниченный keepalive. Нельзя вводить priority
aging, позволяющее менее важному классу обойти ожидающий более важный.

MVP — control plane на постоянном хосте OpenClaw с SQLite WAL очередью ресурса
`mainpc-gpu`; MAIN-PC является только WOL-started executor Ollama. Cloud и CPU
пути, а также существующие GPU-workers Whisper исключены и потребуют отдельного
resource profile. Эта изолированная фаза не меняет callers, трафик,
маршрутизацию, production configuration или source-root order.

Переход возможен, когда representative interactive, photo, cron и batch-video
задания не пересекаются на GPU, cancelled/failed задания освобождают lock, а
восстановление после restart не оставляет зависшей работы.

## Фаза 2 — интеграции

Перевести OpenClaw, Hermes и локальные pipelines на API broker. Блокировать или
сигнализировать о новых прямых вызовах Ollama. Сохранить отдельную cloud-photo
concurrency, поскольку она не использует VRAM MAIN-PC.

Сначала добавить и проверить Ollama-compatible streaming proxy, затем
мигрировать один явно определённый canary caller. Нельзя перенаправлять caller
только потому, что его процесс запущен на OpenClaw или M101.

## Фаза 3 — наблюдаемость и операции

Публиковать глубину и время ожидания очереди, активную нагрузку, priority,
выбранную модель, загруженные модели и VRAM из Ollama `/api/ps`, отказы по
profile limits, отмены и причины model switch. Добавить health checks,
structured logs и операционные dashboards/alerts.

## Фаза 4 — контролируемый rollout и настройка

Раскатывать по классам callers: сначала interactive traffic, затем photo, cron
и batch video. Настраивать profile limits и keepalive по наблюдаемым данным
очереди и VRAM. Сохранить документированный rollback к режиму serialized
lock-and-queue.

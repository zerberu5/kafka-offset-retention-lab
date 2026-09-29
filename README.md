# Kafka Offset-Retention Lab

Mit diesem Setup lässt sich nachstellen, was ein Consumer tut, wenn die committeten
Offsets seiner Gruppe abgelaufen sind (`offsets.retention.minutes`) und
`auto.offset.reset` greift.

| Komponente | Zweck | Zugriff |
|---|---|---|
| `kafka` | apache/kafka 3.9.1, KRaft, Single Broker, Offset-Retention **2 min** | intern `kafka:9092`, Host `localhost:9094` |
| `kafka-ui` | Topics, Offsets, Consumer-Groups ansehen | http://localhost:8090 |
| `producer` | `POST /send?count=N`, legt das Topic an (NewTopic-Bean) | http://localhost:8080 |
| `consumer` | `@KafkaListener` + Logging von Rebalance/Reset | nur Logs |

Alle Consumer-Properties (Group-ID, `auto.offset.reset`, Auto-Commit, AckMode,
Isolation-Level, Log-Level) stehen kommentiert in `docker-compose.yml`. Sie werden
über Platzhalter in `application.yml` gelesen und lassen sich deshalb ohne Rebuild
aus der Shell oder einer `.env`-Datei überschreiben.

> **Wichtig:** `docker-compose start` übernimmt **keine** geänderten Env-Vars.
> Für neue Werte `docker-compose up -d consumer` verwenden, das den Container neu erzeugt.

## Voraussetzungen

- Docker ab 19.03 mit `docker-compose` ab 1.21 (Compose-Dateiformat 2.4). Mit Compose v2
  funktioniert `docker compose` genauso, dort erscheint nur eine harmlose Warnung zu `version`.
- Getestet mit Docker 19.03.13 und docker-compose 1.26 sowie mit aktuellem Docker und Compose v2.
- Wegen Docker 19.03: Die eigenen Images basieren auf Ubuntu 20.04 (`*-focal`). Kafka und
  kafka-ui laufen mit `seccomp:unconfined`, sonst scheitern sie am alten Seccomp-Profil.

## Hintergrund

- Seit Kafka 2.1 (KIP-211) beginnt die Retention erst, wenn die Gruppe **leer** ist
  (State `Empty`, keine aktiven Member). Solange ein Consumer läuft, verfallen die
  Offsets nicht.
- Der Broker prüft alle `offsets.retention.check.interval.ms` (hier 10 s). Die Offsets
  verschwinden also etwa 2 min 10 s, nachdem der letzte Member die Gruppe verlassen hat.
- Danach existiert die Gruppe nicht mehr. Beim nächsten Start findet der Consumer
  keinen Offset und setzt gemäß `auto.offset.reset` zurück:
  - `latest`: Start am Log-Ende. Alles, was in der Zwischenzeit geschrieben wurde, wird
    **übersprungen**.
  - `earliest`: Start am Log-Anfang. Alles noch Vorhandene wird **erneut** verarbeitet.
- Bei `auto.offset.reset=latest` committet Spring Kafka die ermittelte Position direkt bei
  der Zuweisung (`assignmentCommitOption = LATEST_ONLY_NO_TX`). Die Lücke ist damit sofort
  „festgeschrieben“, auch wenn der Consumer danach abstürzt. Im Assignment-Log steht deshalb
  `committed=<log end>`, bei `earliest` dagegen `committed=none`.

Log-Zeilen, die den Reset zeigen:

```
ConsumerCoordinator : Found no committed offset for partition demo-topic-0
SubscriptionState   : Resetting offset for partition demo-topic-0 to position FetchPosition{offset=...}
ConsumerApplication : Partition assigned: demo-topic-0 | log=[0..8) | committed=... | start position=8 = log end | auto.offset.reset=latest
```

Wurde nicht zurückgesetzt, steht dort stattdessen (Logger `ConsumerUtils`)
`Setting offset for partition demo-topic-0 to the committed offset FetchPosition{offset=...}`.

## Reproduktion

Alle Befehle werden im Projektverzeichnis ausgeführt.

### 1. Starten, senden, verarbeiten

```bash
docker-compose up -d --build
curl -X POST 'localhost:8080/send?count=3'
docker-compose logs consumer | grep -E 'Partition assigned|Received partition'
scripts/groups.sh
```

Der Consumer loggt `Received partition=0 offset=0..2`. In der Gruppe steht
`CURRENT-OFFSET 3`, `LAG 0`, und es gibt einen aktiven Member.

### 2. Consumer stoppen

```bash
docker-compose stop consumer
scripts/groups.sh                # "has no active members", CURRENT-OFFSET 3
scripts/groups.sh demo-group --state   # STATE Empty -> ab jetzt läuft die Retention
```

### 3. Warten, bis die Offsets abgelaufen sind

Mindestens `offsets.retention.minutes` (2 min) plus Check-Intervall warten, dann prüfen:

```bash
sleep 150
scripts/groups.sh
# Error: Consumer group 'demo-group' does not exist.
```

Direkt im Container geht das auch ohne Skript:

```bash
docker-compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group demo-group
```

In der kafka-ui ist die Gruppe unter *Consumers* ebenfalls verschwunden.

### 4. Nachrichten senden, während der Consumer gestoppt ist

```bash
curl -X POST 'localhost:8080/send?count=5'   # landen auf Offset 3..7
```

### 5. Consumer starten: die Nachrichten aus Schritt 4 werden übersprungen

```bash
docker-compose start consumer
docker-compose logs --tail=100 consumer | grep -E 'Found no committed|Resetting offset|Partition assigned|Received partition'
```

Erwartet:

```
Found no committed offset for partition demo-topic-0
Resetting offset for partition demo-topic-0 to position FetchPosition{offset=8, ...}
Partition assigned: demo-topic-0 | log=[0..8) | committed=8 | start position=8 = log end | auto.offset.reset=latest
```

Es erscheinen **keine** `Received`-Zeilen für die Offsets 3 bis 7. Erst neue Nachrichten
kommen an:

```bash
curl -X POST 'localhost:8080/send?count=1'   # -> Received ... offset=8
scripts/groups.sh                              # CURRENT-OFFSET 9, LAG 0
```

Offsets 3 bis 7 sind für diese Gruppe verloren, obwohl sie noch im Topic liegen
(prüfbar in der kafka-ui unter *Topics → demo-topic → Messages*).

### 6. Gegenprobe mit `earliest`

```bash
docker-compose stop consumer
sleep 150                                    # Offsets erneut ablaufen lassen
scripts/groups.sh                            # "does not exist"
curl -X POST 'localhost:8080/send?count=2'
KAFKA_AUTO_OFFSET_RESET=earliest docker-compose up -d consumer
docker-compose logs --tail=100 consumer | grep -E 'Resetting offset|Partition assigned|Received partition'
```

Erwartet: `Resetting offset ... to position FetchPosition{offset=0, ...}` und
`start position=0 = log begin`. Danach verarbeitet der Consumer **alle** Nachrichten ab
Offset 0 neu, einschließlich der bereits verarbeiteten (Duplikate).

Zurück zu `latest`: `docker-compose up -d consumer` ohne die Variable.

### 7. Wiederherstellung per `--reset-offsets`

Szenario: Nach Schritt 5 sollen die übersprungenen Offsets 3 bis 7 doch noch verarbeitet
werden. `--reset-offsets` funktioniert nur, wenn die Gruppe **keine aktiven Member** hat.

```bash
docker-compose stop consumer
```

Eine Shell im Kafka-Container öffnen:

```bash
docker-compose exec kafka bash
cd /opt/kafka/bin
```

**Dry-Run** (Standard, ändert nichts, zeigt nur `NEW-OFFSET`):

```bash
# a) auf einen konkreten Offset
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group demo-group \
  --topic demo-topic --reset-offsets --to-offset 3 --dry-run

# b) auf den Log-Anfang
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group demo-group \
  --topic demo-topic --reset-offsets --to-earliest --dry-run

# c) auf einen Zeitpunkt (Record-Timestamp; Format YYYY-MM-DDTHH:mm:SS.sss,
#    ohne Zonenangabe gilt die Zeitzone des Containers = UTC. Alternativ z. B. ...+02:00)
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group demo-group \
  --topic demo-topic --reset-offsets --to-datetime 2026-09-28T09:00:00.000 --dry-run
```

Den Zeitstempel für `--to-datetime` liefert das Payload (`msg-4 @ 2026-09-28T09:12:03`).
Die Container laufen in UTC.

**Ausführen:** den passenden Befehl mit `--execute` statt `--dry-run` wiederholen, z. B.:

```bash
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group demo-group \
  --topic demo-topic --reset-offsets --to-offset 3 --execute
exit
```

Danach **zügig** starten. Die gerade gesetzten Offsets gehören zu einer leeren Gruppe und
verfallen sonst nach 2 min wieder.

```bash
docker-compose start consumer
docker-compose logs --tail=100 consumer | grep -E 'committed offset|Partition assigned|Received partition'
```

Erwartet: `Setting offset for partition demo-topic-0 to the committed offset FetchPosition{offset=3, ...}`,
also kein Reset, danach `Received ... offset=3` bis zum Log-Ende.

Weitere nützliche Varianten: `--shift-by -5`, `--to-latest`, `--to-current`,
`--by-duration PT10M`, `--all-topics`. Mit `--export > plan.csv` lässt sich ein Plan
exportieren und per `--from-file plan.csv` anwenden.

## Aufräumen

```bash
docker-compose down -v
```

## Hilfsskript

`scripts/groups.sh [group] [extra options]` führt `kafka-consumer-groups.sh --describe`
im Kafka-Container aus. Default-Gruppe ist `demo-group`. Beispiele:

```bash
scripts/groups.sh
scripts/groups.sh demo-group --state
scripts/groups.sh demo-group --members --verbose
```

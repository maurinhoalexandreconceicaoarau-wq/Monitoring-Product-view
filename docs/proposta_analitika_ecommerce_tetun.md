# Proposta Projetu

## Sistema Analítika Eventu Komérsiu Elektróniku
### Monitorizasaun ba View, Add to Cart no Sesaun ne'ebé La iha Transasaun Rejistada ho Apache Kafka no Apache Spark

**Naran ofisiál projetu:** *E-Commerce Real-Time Analytics: Monitoring Click, View, and No Purchase Using Apache Kafka and Apache Spark*

**Estudante:** Epifania  
**Instituisaun:** [Prienxe naran instituisaun]  
**Departamentu/programa:** [Prienxe departamentu ka programa]  
**Orientadór/a:** [Prienxe naran orientadór/a]  
**Tinan:** 2026

---

# Kapítulu 1 — Introdusaun

## 1.1 Kontekstu

Loja eletrónika rejista interasaun barak entre kliente no produtu, hanesan haree pajina produtu, tau produtu ba kareta, no halo transasaun. Dadus ida-idak bele sai eventu ida. Bainhira volume eventu aumenta, organizasaun tenke integra fonte, harmoniza schema no alinha tempu atu bele halo análize ne'ebé fiável. Ma et al. (2025), iha estudu kona-ba ROSI, hatudu nesesidade atu integra feature e-commerce hosi kanál oioin ho arquitetura híbrida; Wu no Lin (2018) mós esplora oinsá analytics ba Big Data bele extrai koñesimentu hosi dadus e-commerce ne'ebé la estruturadu. Se empresa analiza eventu sira ne'e hamutuk, nia bele komprende produtu ne'ebé simu atensaun, haree iha etapa ne'ebé prosesu sosa para, no observa produtu ne'ebé iha transasaun rejistada. Informasaun ne'e bele ajuda planeia stock, promosaun, no hadi'a pajina produtu. Maibé, númeru view aas mesak la prova katak produtu ida sei fa'an barak; tenke kompara ho eventu kareta no transasaun.

Projetu ida-ne'e dezenvolve sistema Big Data edukasionál atu hatudu dalan eventu hosi dataset istóriku to'o prosesamentu no dashboard. Sistema uza dataset Retailrocket ne'ebé iha ona iha pasta `data/`, replay eventu sira ba Apache Kafka, prosesu ho Apache Spark Structured Streaming, rai rezultadu iha SQLite no JSON, no aprezenta analiza iha Streamlit. Replay ne'e mak **Historical Event Replay / Real-Time Simulation**; la'ós tráfiku live hosi loja ne'ebé funsiona agora.

## 1.2 Deklarasaun Problema

Empresa ne'ebé iha rejistu view, add to cart no transaction bele hasoru difikuldade atu komprende performansa produtu no dalan kliente nian se eventu sira la organiza no analiza hamutuk. Númeru raw la hatudu ho fasil produtu ne'ebé atrai atensaun, diferensa entre haree no tau ba kareta, ka sesaun ne'ebé remata laiha transasaun rejistada. Tanba ne'e, desizaun kona-ba stock, promosaun no hadi'a pajina produtu bele la iha evidénsia ne'ebé klaru. Aleinde ne'e, prosesu dadus iha sistema Big Data presiza hatudu katak eventu simu duni hosi Kafka, prosesadu duni hosi Spark, no rezultadu rai no hatudu ho traceabilidade.

Problema tékniku ida mak dataset Retailrocket la iha eventu click separadu no la iha naran produtu verifikadu. Ding et al. (2023) uza log interasaun RetailRocket iha peskiza rekomendasaun bazeia ba sesaun; estudu ne'e hatudu valor dataset ba peskiza e-commerce, maibé nia objetivu mak rekomendasaun item tuirmai. Nune'e, sistema tenke analiza eventu `view`, `addtocart` no `transaction` ne'ebé iha, uza `item_id` atu identifika produtu, no hatudu limitasaun sira ho loos. Sistema mos tenke distingue tempu eventu istóriku, tempu ingestion no tempu prosesamentu.

## 1.3 Pergunta Peskiza

1. Oinsá eventu Retailrocket istóriku bele replay liu hosi Apache Kafka no prosesu ho Apache Spark Structured Streaming?
2. Oinsá prosesamentu eventu bele transforma dadus raw sai metrika view, add to cart, transaction no tendénsia produtu ne'ebé bele verifika?
3. Oinsá sesaun ida bele klasifika hanesan sesaun laiha transasaun rejistada, no saida mak limitasaun interpretasaun klasifikasaun ne'e?
4. Oinsá dashboard bele hatudu eventu ne'ebé simu no prosesadu, kualidade dadus, no estadu Kafka/Spark bazeia ba evidénsia real?

## 1.4 Objetivu Projetu

### Objetivu Jerál

Dezenvolve no demonstra sistema analítika eventu komérsiu eletróniku ne'ebé replay dadus Retailrocket istóriku liu hosi Kafka, prosesu ho Spark, rai rezultadu iha SQLite, no hatudu analiza produtu no pipeline iha dashboard Streamlit.

### Objetivu Espesífiku

1. Halo auditoria ba dataset orijinál no dokumenta tipu eventu, kampu, kontajen no limitasaun dadus.
2. Implementa replay producer ho taxa konfigurável no validasaun ba mensajen ne'ebé haruka ba topic `retailrocket-events`.
3. Implementa Spark Structured Streaming atu lee, valida, deduplika, agrega no rai eventu ne'ebé válidu.
4. Define regra sesaun no analiza sesaun elegível ne'ebé la iha transasaun rejistada.
5. Hatudu eventu, KPI, analiza produtu, funil sosa, kualidade dadus no estadu pipeline iha dashboard ne'ebé bazeia ba output backend.
6. Verifika pipeline ho log, Kafka offset/ACK, Spark batch, output SQLite/JSON no timestamp sira.

## 1.5 Eskopu Projetu

Projetu uza dataset Retailrocket ne'ebé iha ona; la troka dataset ho dadus random no la muda ficheiru orijinál. Foku prosesamentu mak eventu `view`, `addtocart` no `transaction`. Click genuínu la disponivel iha fonte, nune'e click sei marka hanesan la disponivel to'o iha fonte click ne'ebé autentiku. Naran produtu la disponivel; análize produtu sei uza `item_id`. Replay ne'e simulasaun bazeia ba dadus istóriku, la'ós integrasaun ho loja live.

Sesaun kria tuir visitante ida-idak no ordem timestamp; se iha intervalu inatividade liu 30 minutu, sistema hahú sesaun foun. Sesaun elegível tenke iha view ida ka add to cart ida. Sesaun elegível ida klasifika “laiha transasaun rejistada” bainhira la iha eventu transaction iha sesaun ne'e. Ida-ne'e la prova katak visitante nunka sosa iha tempu seluk ka loja seluk.

Implementasaun atual mak ambiente demonstrasaun lokál ho Docker Compose, Kafka broker ida, Spark lokál, SQLite lokál no dashboard Streamlit iha host. Eskopu la inklui disponibilidade aas, cluster multi-broker, autenticasaun produsaun, click real, product title verifikadu, ka modelu Machine Learning atu prediz komportamentu.

## 1.6 Benefísiu Projetu

- **Ba estudante:** aprende no demonstra pipeline Big Data hosi fonte to'o dashboard, inklui replay, mensajen, processamento distribuidu no persisténsia.
- **Ba dosente/evaluadór:** bele haree amostra dadus, log, status, eventu Kafka, rezultadu Spark no regra analítika ne'ebé dokumenta.
- **Ba utilizadór negósiu iha demonstrasaun:** hetan ezemplu oinsá komparasaun view, kareta no transasaun bele ajuda organiza diskusaun kona-ba produtu no prosesu sosa.
- **Ba dezenvolvimentu futuru:** arkitetura bele simu fonte web ne'ebé fó click genuínu no metadadu produtu ne'ebé verifikadu, se fonte sira ne'e disponivel.

# Kapítulu 2 — Revizaun Literatura

## 2.1 Big Data

Big Data refere ba dadus no prosesamentu ne'ebé presiza kapasidade atu jere volume, variedade, velocidade no kualidade dadus ne'ebé aplikasaun konvensionál bele la suporta ho di'ak. Iha komérsiu eletróniku, eventu utilizadór bele akumula hanesan rejistu interasaun, no nia valor depende ba kapasidade atu organiza, analiza no interpreta rezultadu ba desizaun. Stahl et al. (2021) mapeia teknolojia no paradigma ba analytics iha stream, no esplika ligasaun entre fonte eventu, plataforma no análize iha tempu real. Iha e-commerce, pipeline analytics presiza integra dadus ho schema no tempu ne'ebé konsistente; Ma et al. (2025) demonstra nesesidade ne'e iha integrasaun feature entre kanál. Ambiente lokál ida-ne'e la'ós prova katak sistema iha eskala produsaun ka toleránsia falha enterprise.

Iha projetu ida-ne'e, termu Big Data refere ba pipeline edukasionál ne'ebé trata dataset eventu ho millaun rejistu no demonstra komponentes ba ingestion, prosesamentu stream, armazenamento no visualizasaun. Ambiente lokál ida-ne'e la'ós prova katak sistema iha eskala produsaun ka toleránsia falha enterprise.

## 2.2 Fluxu Dadus (Data Streaming)

Data streaming mak prosesamentu eventu ne'ebé tama kontinuamente ka iha parte ki'ik, atu aplikasaun bele trata dadus antes fonte remata (Stahl et al., 2021; van Dongen & Van den Poel, 2020; Samosir et al., 2016). Framework sira iha trade-off entre laténsia, throughput no konsumu rekursu; nune'e, “real-time” tenke interpreta tuir medisaun no nesesidade aplikasaun (van Dongen & Van den Poel, 2020). Iha projetu ida-ne'e, fonte mak dadus istóriku, nune'e producer replay eventu sira iha lote no taxa ne'ebé bele regula. Sistema hatudu **simulasaun iha tempu real**: oras eventu orijinál sei hela iha tinan 2015, maibé oras ingestion no prosesamentu Spark mak tempu replay atual. Tanba ne'e, dashboard tenke hatudu timestamp sira ne'e separadamente.

## 2.3 Apache Kafka

Apache Kafka organiza eventu iha topic. Producer hakerek eventu ba topic, broker rai eventu tuir konfigurasaun retensaun, no consumer lee eventu tuir offset. Arquitetura producer–broker–consumer permite separa aplikasaun ne'ebé haruka eventu hosi aplikasaun ne'ebé lee no prosesu eventu (Wiatr et al., 2018; Raptis et al., 2024). Partisaun bele hasa'e paralelismu, maibé nia númeru no broker sira presiza planeia tuir volume, rekursu no objetivu servisu (Raptis et al., 2024).

Iha sistema ida-ne'e, replay producer haruka mensajen JSON ba topic `retailrocket-events`. Kada mensajen lori event ID no metadata replay. Producer simu Kafka acknowledgment no partition/offset bainhira publikasaun susesu. Dashboard tenke hatudu broker reachability, kontajen publikasaun no erro bazeia ba observasaun real; konfigurasaun de'it la signifika Kafka online.

## 2.4 Apache Spark

Apache Spark mak engine ba prosesamentu dadus distribuidu. Spark Structured Streaming permite trata stream hanesan tabela ne'ebé atualiza bainhira eventu foun tama. Avaliasaun van Dongen no Van den Poel (2020) kompara framework stream processing no diskute trade-off laténsia, throughput no konsumu rekursu; nia rezultadu ajuda atu interpreta performance tuir workload, la'ós atu promete laténsia ida ba konfigurasaun hotu. Wang et al. (2022) estuda dezafiu ne'ebé desenvolvedor sira hasoru iha aplikasaun Spark, inklui transformasaun, konfigurasaaun no I/O. Iha proposta ida-ne'e, Spark Structured Streaming lee Kafka, valida no deduplika eventu, agregadu iha micro-batch no hakerek output. Checkpoint no toleránsia falha tenke relata tuir konfigurasaun ne'ebé implementa, la'ós hanesan garantia ne'ebé sistema lokal ida-ne'e seidauk demonstra.

Spark iha projetu ida-ne'e lee Kafka, parse payload tuir schema, valida kampu no tipu eventu, deduplika tuir `event_id`, halo agregasaun ba tipu eventu no hakerek output prosesadu. Heartbeat no informasaun batch uza atu hatudu se Spark foin prosesu duni; estado “running” ne'ebé rai uluk mesak la prova katak processor sei moris.

## 2.5 Analítika Dadus

Data analytics mak prosesu transforma dadus sai informasaun ne'ebé bele uza atu responde pergunta. Análize deskriptivu hatudu saida mak rejista ona, por ezemplu total view, add to cart no transaction; komparasaun produtu hatudu `item_id` ne'ebé hetan eventu barak; funil kompara etapa sira; no análize sesaun klasifika sesaun ne'ebé la iha transasaun rejistada tuir regra ne'ebé dokumenta. Estudu sira kona-ba pipeline analytics iha e-commerce hatudu katak konsisténsia schema, alinhamentu tempu no fonte dadus importante atu evita interpretasaun sala (Ma et al., 2025; Wu & Lin, 2018).

Projetu ida-ne'e la treina modelu predisaun. Nune'e, rezultadu la tenke bolu predisaun, propensaun atu sosa ka kauzalidade. Eventu ida bele repete hosi visitante ida, no event count la hanesan ho unique visitor ka taxa konversaun individual.

## 2.6 Projetu Relasionadu

Retailrocket E-commerce Dataset uza ona iha peskiza rekomendasaun bazeia ba sesaun. Estudu HyperS²Rec testa modelu rekomendasaun ho RetailRocket no dataset sosa online seluk (Ding et al., 2023). Estudu ne'e foka ba predisaun item tuirmai, maibé projetu atual foka ba pipeline eventu no análize deskriptivu; sistema ida-ne'e la treina modelu hanesan HyperS²Rec.

ROSI mak projetu relasionadu ne'ebé dezenvolve pipeline híbrida atu integra eventu online ne'ebé tama iha tempu real ho dadus transasaun loja ne'ebé prosesu iha lote. Estudu ne'e hatudu katak schema, sincronizasaun no tempu atualizasaun tenke konsidera bainhira integra fonte oioin (Ma et al., 2025). Peskiza kona-ba Kafka analiza impaktu konfigurasaun producer ba laténsia no rekursu (Wiatr et al., 2018), no estudu seluk avalia partisaun topic no númeru broker atu atende objetivu aplikasaun (Raptis et al., 2024).

Projetu ida-ne'e la replika ROSI ka harii rekomendadór. Nia foku mak demonstrasaun pipeline replay istóriku Kafka–Spark, rastreabilidade eventu, análize deskriptivu no dashboard monitorizasaun. Nu'udar projetu edukasionál, nia hatudu oinsá etapa ingestion, validasaun, deduplikasaun, agregasaun no visualizasaun bele liga iha sistema ida, ho limitasaun eskala lokál ne'ebé deklaradu.

# Kapítulu 3 — Metodolojia

## 3.1 Fonte Dadus

Fonte prinsipál mak Retailrocket E-commerce Dataset ne'ebé download ona iha pasta `data/`. `events.csv` iha koluna `timestamp`, `visitorid`, `event`, `itemid` no `transactionid`. Fonte ne'ebé audita iha projetu iha 2,756,101 rejistu: 2,664,312 `view`, 69,332 `addtocart` no 22,457 `transaction`, tuir audit lokál. Artigu kona-ba rekomendasaun ho RetailRocket konfirma dataset ne'e nu'udar log interasaun e-commerce ne'ebé uza iha peskiza (Ding et al., 2023); deskrisaun no ficheiru orijinál mai hosi distribuisaun dataset (Retailrocket, n.d.). ID transasaun mamuk iha rejistu barak tanba ID ne'e relevante liu ba eventu transaction; mamuk ida-ne'e la'ós automaticamente erro.

Fonte suporta `category_tree.csv` no `item_properties_part1.csv`/`item_properties_part2.csv` ba informasaun adisionál, maibé sira la fó naran produtu verifikadu. Análize iha proposta ida-ne'e uza `item_id` atu identifika item. Ficheiru orijinál sei hela la muda; output hotu rai iha `artifacts/`.

## 3.2 Rekolla Dadus

Projetu la halo rekolla dadus foun hosi kliente real. Dadus rekolla tiha ona iha dataset Retailrocket no projetu lee kopia lokál. `events.csv` mak fonte ba replay. Auditoria fonte konta rejistu, tipu eventu no valor ne'ebé falta atu estabelese baseline antes ingestion. Replay konfigurável sei hili rejistu tuir limite no skip ne'ebé utilizadór husu; producer ordena rejistu tuir timestamp molok kria sesaun no publika eventu.

## 3.3 Injesaun Dadus

Replay producer transforma linha CSV ba eventu JSON ho `event_id`, `event_time`, `event_time_ms`, `ingested_at`, `visitor_id`, `session_id`, `event_type`, `item_id`, `transaction_id`, `source` no `capture_id`. `event_id` bazeia ba orijen rejistu atu nune'e bele rastreia fonte no deduplika. `event_time` muda hosi timestamp millisecond ba data/hora UTC; `ingested_at` rejista tempu simu/publikasaun. Producer haruka eventu ba topic Kafka `retailrocket-events`, simu acknowledgment, no rai metadata hanesan partition no offset.

Taxa replay bele configura atu laptop bele suporta. Mensajen ne'ebé la valida ka Kafka ne'ebé la asesível tenke hatudu erro; sistema la bele aumenta KPI hanesan se publikasaun susesu bainhira la iha Kafka ACK.

## 3.4 Armazenamentu Dadus

Kafka rai eventu iha topic durante tempu retensaun ne'ebé konfiguradu. Spark hakerek output validu iha JSON iha `artifacts/`. Aplikasaun dashboard espelha eventu Spark ne'ebé kometadu ba SQLite iha `artifacts/retailrocket_analytics.db`.

Tabela `processed_events` rai eventu prosesadu, timestamp, Kafka metadata, batch ID no status preprocessing; `event_id` uza hanesan chave atu previne rejistu duplikadu iha database. Tabela `spark_batches` rai sumarizasaun batch hanesan total simu, válidu, invalidu, duplikadu no durasaun. Métrika runtime no heartbeat rai iha ficheiru artefaktu adisionál. `data/` la uza atu rai output.

## 3.5 Prosesamentu Dadus

Pré-prosesamentu hala'o iha etapa rua:

1. **Iha replay producer:** lee kampu orijinál; ordena rejistu tuir timestamp millisecond; kria ID eventu; muda oras eventu ba UTC; kria `session_id` tuir visitor; komplementa payload ho tempu ingestion no metadata replay.
2. **Iha Spark Structured Streaming:** lee Kafka stream no parse JSON tuir schema; valida `event_id`, oras eventu, oras millisecond pozitivu, visitor, session, item, no event type ida hosi `view`, `addtocart`, `transaction`; filtra rejistu invalidu; deduplika tuir `event_id`; agrega total eventu; hakerek eventu válidu no batch result ba output.

Sesaun forma tuir visitor no timestamp. Bainhira gap entre eventu rua boot liu 30 minutu, eventu tuirmai hahú sesaun foun. Sesaun elegível tenke iha pelumenus view ida ka add to cart ida. Bainhira sesaun ne'e la iha transaction event, nia marka hanesan sesaun laiha transasaun rejistada. Klasifikasaun ne'e limita ba sesaun iha dadus; la halo alegasaun kona-ba visitante nia komportamentu iha tempu ka loja seluk.

## 3.6 Análize Dadus

Análize sei hala'o iha nivel eventu, item, visitor no sesaun, ho unidade ida-idak hatudu klaru:

- Konta eventu hotu no fahe ba view, add to cart no transaction.
- Kompara `item_id` tuir view, add to cart no transaction event count.
- Hatudu unique visitor no unique item ketak hosi event count.
- Hatudu funil view → add to cart → transaction ho avizu katak event count la reprezenta ema ida-idak nia konversaun.
- Konta sesaun elegível no sesaun laiha transasaun rejistada; persentajen uza sesaun elegível hanesan denominator.
- Hatudu kontajen invalidu, duplikadu, Kafka ACK, Spark processed no batch outputs ketak, tanba sira iha unidade diferente.

Click monitoring sei marka hanesan la disponivel iha fonte atual. Trend chart sei agrega eventu ne'ebé prosesadu tuir event time ka processing time tuir pergunta; chart sei hatudu base tempu ne'ebé uza.

## 3.7 Vizualizasaun

Dashboard Streamlit sei lee output backend/SQLite no artefaktu pipeline, la'ós métrika estátiku hardcoded. Seksaun dashboard mak Overview, Click & View Monitoring, Event Stream, Purchase Funnel, No-Purchase Analysis, Product Analytics, Pipeline Monitoring no Data Quality. Chart no tabela sei hatudu event type, `item_id`, event time, ingestion time no processing time bainhira disponivel.

Refresh dashboard sei akontese de'it bainhira replay, Spark startup ka batch prosesamentu ativa. Kafka status bazeia ba broker reachability; Spark status bazeia ba estadu runtime no heartbeat foun. Dashboard tenke hatudu status parado/erro bainhira servisu para, no tenke label replay hanesan simulasaun istóriku.

## 3.8 Dezenhu no Avaliasaun Peskiza

Peskiza ida-ne'e mak dezenvolvimentu sistema ho análize deskriptivu. Avaliasaun sei haree se kada objetivu iha evidénsia ne'ebé bele repete: (1) auditoria CSV bele rekalkula total no tipu eventu; (2) replay producer publika payload no hetan Kafka acknowledgement; (3) Spark rejista output válidu, invalidu no duplikadu ho batch ID; (4) eventu ne'ebé Spark kometadu bele hetan iha JSON no SQLite tuir `event_id`; no (5) dashboard hatudu metrika no estadu pipeline tuir output real. Avaliasaun framework sira hatudu katak laténsia, throughput no uza rekursu mak medida importante, maibé sira depende ba workload no ambiente (van Dongen & Van den Poel, 2020; Samosir et al., 2016). Nune'e, relatóriu sei hatudu medisaun replay no batch ho kondisaun teste, la halo alegasaun katak rezultadu ida-ne'e aplika ba produsaun.

Kualidade pipeline sei analiza ho taxa rejistu inválidu, taxa duplikadu, kontajen eventu tuir tipu, diferensa entre eventu husu, ACK producer, Kafka offset no eventu Spark uniku. Diferensa sira ne'e sei esplika tanba unidade medida la hanesan. La sei trata la hetan transasaun iha sesaun ida hanesan prova katak utilizadór la sosa iha loja seluk ka iha tempu seluk.

# Kapítulu 4 — Dezenhu Sistema

## 4.1 Arkitetura Sistema

Arquitetura ne'e hatudu dalan implementadu hosi dataset Retailrocket to'o dashboard. Komponente sira mak lokál; dashboard consulta artefaktu output no status ne'ebé pipeline produz.

:::diagram
node|01|Dataset istóriku|data/events.csv: timestamp, visitorid, event, itemid, transactionid; fonte raw la muda.
flow|Producer lee rejistu, ordena timestamp no prepara identificadór sesaun/eventu.
node|02|Replay Producer|Normaliza eventu ba JSON; aumenta event_id, session_id, event_time, ingested_at no capture_id.
flow|Payload JSON publika ba topic. Kafka ACK, partition no offset rejista evidénsia ingestion.
node|03|Apache Kafka|Broker lokál iha Docker Compose; topic retailrocket-events.
flow|Spark lee mensajen hosi offset no parse tuir schema eventu.
node|04|Spark Structured Streaming|Valida kampu/tipu, filtra invalidu, deduplika event_id, agrega no kria batch summary.
flow|Output ne'ebé kometadu hakerek ba JSON no persiste ba SQLite.
node|05|Output no Storage|artifacts/: JSON output no metrics; SQLite: processed_events no spark_batches.
flow|Dashboard lee dadus kometadu, batch metrics, heartbeat no estadu servisu.
node|06|Dashboard Streamlit|Overview, Event Stream, Funnel, Session Analysis, Product Analytics, Pipeline Monitoring no Data Quality.
:::

Kafka no Spark funsiona iha Docker Compose; Streamlit funsiona ho Python iha host. Sistema demonstra pipeline lokál ho broker ida no la pretende atu sai arquitetura produsaun redundante.

## 4.2 Diagrama Fluxu Dadus

### Nivel 0 — Diagrama Konteksu

Diagrama konteksu hatudu limite sistema no entidade ne'ebé troka dadus ho sistema. Dataset fó eventu, utilizadór hahú replay no husu vizualizasaun, no sistema devolve dashboard ho rezultadu no status pipeline.

:::diagram
node|E1|Fonte dadus|Retailrocket events.csv: rejistu istóriku no kampu raw.
flow|Sistema lee no replay eventu tuir limite no taxa ne'ebé configura.
node|P0|Sistema Analítika Eventu|Replay Producer → Kafka → Spark → JSON/SQLite; valida, deduplika, agrega no regista status.
flow|Dashboard fó KPI, trend, event trace, data quality no estadu pipeline.
node|E2|Utilizadór / Analista|Hili capture/replay, limita eventu, observa vizualizasaun no lee evidénsia.
:::

### Nivel 1 — Fluxu Internu

Iha nivel ida-ne'e, prosesu no armazenamento internas distingue. `event_id` ajuda atu liga eventu iha fonte, Kafka, output Spark no tabela SQLite; ACK Kafka, Spark output no SQLite commit mak evidénsia ketak.

:::diagram
node|D1|Fonte CSV|events.csv iha data/: event raw no timestamp original.
flow|P1 lee, ordena no normaliza; kria event_id/session_id no payload JSON.
node|P1|Replay Producer|Validasaun input, konfigurasaun replay, publish no captura ACK.
flow|Kafka simu payload; atribui partition/offset no rai tuir retensaun.
node|D2|Kafka Topic|retailrocket-events: event JSON no offset.
flow|P2 lee offset; parse schema, valida, deduplika event_id no agrega micro-batch.
node|P2|Spark Processor|process eventu, output event rows, metricas batch no status.
flow|Output commit ba artifaktu JSON no SQLite; raw CSV nafatin intactu.
node|D3|JSON + SQLite|processed_events no spark_batches iha artifacts/.
flow|P3 consulta outputs, agrega KPI no lee runtime health/heartbeat.
node|P3|Dashboard Streamlit|Hatudu event stream, análize deskriptivu, kualidade no status ba utilizadór.
flow|Utilizadór hahú replay no lee rezultadu; sistema la manda eventu ba fonte externa.
node|E1|Utilizadór|Analista ka estudante ne'ebé uza dashboard iha ambiente lokál.
:::

## 4.3 Dezenhu Baze Dadus

Database lokál mak SQLite, ho tabela prinsipál rua:

| Tabela | Kampu prinsipál | Funsaun |
|---|---|---|
| `processed_events` | `event_id` (PK), `event_time`, `ingested_at`, `processed_at`, `visitor_id`, `session_id`, `event_type`, `item_id`, `transaction_id`, `kafka_partition`, `kafka_offset`, `spark_batch_id`, `preprocessing_status` | Rai eventu Spark ne'ebé validu no trace pipeline. `event_id` previne rejistu repetidu. |
| `spark_batches` | `batch_id` (PK), `received`, `valid`, `invalid`, `duplicates`, `processing_seconds`, `processed_at` | Rai rezultadu no durasaun kada micro-batch Spark. |

Metric aggregate bele kalkula hosi event rows no batch summaries. Database lokál iha `artifacts/`; nia output bele harii fali husi replay, no la substitui dataset raw.

## 4.4 Etapa Prosesamentu

1. Utilizadór husu capture/replay iha dashboard.
2. Backend valida númeru eventu no taxa replay, depois hahú producer.
3. Producer lee no ordena rejistu CSV; atribui event ID no session ID; haruka payload ba Kafka.
4. Kafka simu payload, aloka partition no offset, no fó acknowledgement ba producer.
5. Spark Structured Streaming lee topic, parse schema no rejista processing time.
6. Spark valida kampu obrigatóriu, filtra tipu ne'ebé la suporta no deduplika tuir `event_id`.
7. Spark agrega eventu no hakerek JSON; dashboard persist output ne'ebé kometadu ba SQLite.
8. Dashboard atualiza event stream no grafiku tuir output foun no estadu runtime real.

Se etapa ida falla, status no erro tenke hatudu iha dashboard/logs. “Completed” ba replay la signifika katak Spark sei running; ida-idak iha status rasik.

## 4.5 Dezenhu Painel Monitorizasaun

- **Overview:** total eventu prosesadu, view, add to cart, transaction, sesaun elegível no sesaun laiha transasaun rejistada.
- **Click & View:** click marca hanesan la disponivel; view uza eventu orijinál.
- **Event Stream:** amostra payload eventu no event, ingestion, Kafka ACK/offset no Spark processing info.
- **Purchase Funnel:** view, add to cart no transaction; esplika katak total eventu repete bele mai hosi visitor hanesan.
- **No-Purchase Analysis:** regra sesaun 30 minutu, denominator sesaun elegível, no interpretasaun limitadu.
- **Product Analytics:** ranking `item_id` ba view/cart/transaction, la inventa naran produtu.
- **Pipeline Monitoring:** Kafka broker, replay producer, Spark heartbeat/batch, kontajen no erro.
- **Data Quality:** rejistu raw, missing values, valid, invalid, duplikadu no resultadu preprocessing.

Dashboard sei uza tema visual consistente ho tipografia no kontraste ne'ebé fasil lee. Grafiku sei hato'o pergunta analítika klaru—hanesan tendénsia eventu tuir tempu ka komparasaun produtu tuir tipu interasaun—no la'ós dekorasaun de'it.

## 4.6 Seguransa, Privasidade no Limite Sistema

Dadus Retailrocket uza ID visitante no item ne'ebé anónimu iha ficheiru fonte. Projetu sei trata dataset iha ambiente lokál, la tenta re-identifika ema no la haruka dadus ba servisu publiku. Servisu Docker liga ba rede lokál; dashboard limita ba loopback. Eskopu demonstrasaun iha broker ida, partisaun ida, Spark local mode no storage lokál, nune'e la iha redundánsia, autentikasaun produsaun, armazenamento durável partilladu ka alta disponibilidade. Peskiza kona-ba Kafka hatudu katak partisaun no númeru broker presiza planeia tuir rekursu no objetivu fiabilidade (Raptis et al., 2024); proposta ida-ne'e dokumenta limitasaun ne'e atu labele interpreta protótipu lokál hanesan plataforma produsaun.

# Kapítulu 5 — Rezultadu Esperadu

Estudante espera atu hetan sistema demonstrasaun ida ne'ebé hatudu dalan kompletu hosi dataset Retailrocket to'o Kafka, Spark, SQLite no dashboard. Rezultadu ne'ebé espera inklui:

1. Auditoria dataset ho kontajen eventu ne'ebé bele repete, separasaun event type no dokumentasaun limitasaun click/naran produtu.
2. Replay producer ne'ebé bele kontrola no fó evidénsia payload, Kafka acknowledgment, partition no offset.
3. Spark Structured Streaming ne'ebé hatudu schema validation, kontajen valid/invalid/duplicate, agregasaun, no output JSON ne'ebé bele haree.
4. SQLite database ne'ebé rai eventu Spark no batch metadata ho trace timestamp.
5. Dashboard Streamlit ne'ebé hatudu update husi output backend bainhira replay prosesu, no hatudu estadu loos bainhira pipeline idle/erro.
6. Análize deskriptivu ba view, add to cart, transaction, item ID, funnel no sesaun laiha transasaun rejistada tuir regra 30 minutu.
7. Evidénsia demonstrasaun ba dosente: log producer/Spark, Kafka offset/counts, sample payload, batch summary, registu SQLite no screenshot dashboard.

Rezultadu sei konsidera kompletu bainhira amostra eventu ida bele tuir hosi rejistu orijinál, liu ba payload replay no metadata Kafka, to'o batch Spark no linha SQLite ho `event_id` hanesan; definisaun metrika no unidade sira mos tenke dokumenta. Replay no dashboard tenke hatudu estadu ativa, remata ka iha erro tuir evidénsia runtime, no kontajen sira tenke relata ketak bainhira unidade medida la hanesan.

Kontajen finais sei depende ba replay ne'ebé hala'o no tenke rekonsilia tuir unidade: rejistu husu, ACK Kafka, Kafka retained offsets, eventu ID uniku ne'ebé Spark kometadu no eventu rai iha SQLite. Sistema la promete click analytics, naran produtu, tráfiku loja live ka predisaun Machine Learning, tanba fonte no implementasaun atual la suporta rezultadu hirak ne'e.

# Estrutura Relatóriu Final

Relatóriu final sei organiza hanesan tuir mai. Kapítulu sira tenke liga ba pergunta peskiza no uza evidénsia implementasaun, la'ós dehan de'it katak sistema funsiona.

1. **Kapítulu 1 — Introdusaun:** kontekstu komérsiu eletróniku, deklarasaun problema, pergunta peskiza, objetivu jerál no espesífiku, eskopu, limitasaun fonte no benefísiu.
2. **Kapítulu 2 — Revizaun Literatura:** Big Data no data analytics; data streaming; Apache Kafka; Apache Spark Structured Streaming; metrika avaliasaun pipeline; projetu relasionadu kona-ba event streaming, e-commerce analytics no RetailRocket. Kapítulu ne'e tenke esplika diferensa entre literatura no sistema ne'ebé dezenvolve.
3. **Kapítulu 3 — Metodolojia Peskiza:** tipu peskiza/dezenvolvimentu sistema, fonte dadus, rekolla no auditoria, ingestion, storage, regras pré-prosesamentu, análize, dashboard no planu avaliasaun. Hatama unidade análize, definisaun sesaun 30 minutu, KPI no medida kualidade dadus.
4. **Kapítulu 4 — Dezenhu Sistema no Implementasaun:** arkitetura; DFD nivel kontektu no prosesu; schema eventu; dezenhu tabela SQLite; processing pipeline; Docker Compose; dashboard; konfigurasaun replay; mekanizmu deduplikasaun no traceabilidade; seguransa no limitasaun ambiente.
5. **Kapítulu 5 — Rezultadu no Diskusaun:** deskrisaun dataset no audit; konfigura demonstrasaun; rezultadu Kafka ACK; batch Spark; output JSON/SQLite; invalidu/duplikadu; trend, funnel, item no sesaun; komparasaun rezultadu ho objetivu; diskute diferensa kontajen no limitasaun interpretasaun. Inklui screenshot/log hanesan evidénsia, no evita inventa rezultadu ka laténsia ne'ebé la medidu.
6. **Kapítulu 6 — Konkluzaun no Rekomendasaun:** responde pergunta peskiza ida-idak, resume objetivu ne'ebé atinji, limita konkluzaun tuir fonte no ambiente, no fó etapa futuru hanesan fonte click autentiku, naran produtu verifikadu, broker barak, storage durável no alerta operasionál.
7. **Referénsia sira:** lista bibliografia ho estilo ida ne'ebé konsistente; separa artigu akademiku hosi ScienceDirect/IEEE Xplore husi dataset no dokumentasaun teknika.
8. **Apéndise sira:** amostra CSV/payload; data dictionary/schema; DFD no dezenhu database; log replay/Kafka/Spark; test cases no rezultadu; screenshot dashboard; instrusaun atu repete demo.

# Referénsia sira

## Artigu Akademiku hosi IEEE Xplore no ScienceDirect

Lista tuir mai uza forma bibliográfika autor–tinan. Iha teksu, sitasaun narativa bolu autor no tinan; sitasaun parentétika hatama autor no tinan atu leitor bele buka entrada kompleta iha lista ida-ne'e.

- Ding, C., Zhao, Z., Li, C., Yu, Y., & Zeng, Q. (2023). Session-based recommendation with hypergraph convolutional networks and sequential information embeddings. *Expert Systems with Applications, 223*, Article 119875. https://doi.org/10.1016/j.eswa.2023.119875. (ScienceDirect.)
- Ma, L., Tang, S., Ganesh, A., Chen, J., Padmanabhan, A., Patel, M., Xu, J., Cho, J., Korpeoglu, E., Kumar, S., & Achan, K. (2025). ROSI: A hybrid solution for omni-channel feature integration in E-commerce. *Data & Knowledge Engineering*, Article 102465. https://doi.org/10.1016/j.datak.2025.102465. (ScienceDirect.)
- Raptis, T. P., Cicconetti, C., & Passarella, A. (2024). Efficient topic partitioning of Apache Kafka for high-reliability real-time data streaming applications. *Future Generation Computer Systems, 154*, 173–188. https://doi.org/10.1016/j.future.2023.12.028. (ScienceDirect.)
- Samosir, J., Indrawan-Santiago, M., & Delir Haghighi, P. (2016). An evaluation of data stream processing systems for data driven applications. *Procedia Computer Science, 80*, 439–449. https://doi.org/10.1016/j.procs.2016.05.322. (ScienceDirect.)
- Stahl, F. T., Roesch, E., & Dubuc, T. (2021). Mapping the Big Data landscape: Technologies, platforms and paradigms for real-time analytics of data streams. *IEEE Access, 9*, 15351–15374. https://doi.org/10.1109/ACCESS.2020.3046132. (IEEE Xplore.)
- van Dongen, G., & Van den Poel, D. (2020). Evaluation of stream processing frameworks. *IEEE Transactions on Parallel and Distributed Systems, 31*(8), 1845–1858. https://doi.org/10.1109/TPDS.2020.2978480. (IEEE Xplore.)
- Wang, Z., Chen, T.-H. (Peter), Zhang, H., & Wang, S. (2022). An empirical study on the challenges that developers encounter when developing Apache Spark applications. *Journal of Systems and Software, 194*, Article 111488. https://doi.org/10.1016/j.jss.2022.111488. (ScienceDirect.)
- Wiatr, R., Słota, R., & Kitowski, J. (2018). Optimising Kafka for stream processing in latency sensitive systems. *Procedia Computer Science, 136*, 99–108. https://doi.org/10.1016/j.procs.2018.08.242. (ScienceDirect.)
- Wu, P.-J., & Lin, K.-C. (2018). Unstructured big data analytics for retrieving e-commerce logistics knowledge. *Telematics and Informatics, 35*(1), 237–244. https://doi.org/10.1016/j.tele.2017.11.004. (ScienceDirect.)

## Fonte Dataset no Dokumentasaun Teknika

Retailrocket. (n.d.). *Retailrocket recommender system dataset* [Data set]. Kaggle. https://www.kaggle.com/retailrocket/ecommerce-dataset/home. Kontajen iha proposta mai hosi auditoria lokal ba ficheiru `events.csv` iha projetu.

Apache Software Foundation. (n.d.). *Apache Kafka documentation*. https://kafka.apache.org/documentation/; Apache Software Foundation. (n.d.). *Structured Streaming programming guide*. https://spark.apache.org/docs/latest/streaming/index.html.

Governu Repúblika Demokrátika Timor-Leste. (2004). *Dekretu Governu n.º 1/2004 kona-ba Ortografia Padrãu Lian Tetun*. https://mj.gov.tl/jornal/lawsTL/RDTL-Law/RDTL-Gov-Decrees/Gov-Decree-2004-01.pdf.

# Apéndise sira ba Sistema

Apéndise sira-ne'e dokumenta artefaktu ne'ebé liga diretamente ba implementasaun. Bainhira entrega relatóriu final, estudante tenke substitui instrusaun ba evidénsia real husi run; labele inventa log, output ka screenshot.

## Apéndise A — Estrutura Dadus no Ezemplu Eventu

CSV raw iha koluna `timestamp`, `visitorid`, `event`, `itemid` no `transactionid`. Payload replay aumenta metadadu atu rastreia eventu. Ezemplu ida-ne'e hatudu estrutura; valor entre < > tenke troka ho valor husi rejistu real ne'ebé hili atu demonstra.

```json
{
  "event_id": "<capture_id>:<row_id>",
  "event_time": "<UTC timestamp husi timestamp raw>",
  "event_time_ms": 0,
  "ingested_at": "<UTC timestamp publika>",
  "visitor_id": "<visitorid raw>",
  "session_id": "<visitorid + regra inatividade 30 minutu>",
  "event_type": "<view | addtocart | transaction>",
  "item_id": "<itemid raw>",
  "transaction_id": "<transactionid ka null>",
  "source": "retailrocket",
  "capture_id": "<identifikadór replay>"
}
```

Tabela A1. Kampu raw no kampu normalizado tenke liga tuir regra mapping iha producer. `event_time` mai hosi timestamp raw; `ingested_at` mai hosi tempu publikasaun. Labele uza sira atu substitui malu iha trend chart.

## Apéndise B — Data Dictionary no Storage

| Elementu | Kampu | Definisaun / unidade | Evidénsia atu hatama |
|---|---|---|---|
| Payload eventu | `event_id` | Identifikadór úniku ba eventu replay; uza ba deduplikasaun no trace | Payload exemplo no query tuir ID |
| Payload eventu | `event_time`, `event_time_ms` | Oras eventu original husi dataset, iha UTC/millisecond | Raw row no payload pareadu |
| Payload eventu | `ingested_at` | Oras producer publika eventu | Producer log / payload |
| Payload eventu | `visitor_id`, `session_id` | Visitor raw no sesaun tuir regra gap > 30 minutu | Sesionização screenshot/record |
| Payload eventu | `event_type`, `item_id`, `transaction_id` | Event raw normalizado, item raw no ID transasaun opcional | Schema payload |
| SQLite `processed_events` | `event_id` (PK), event/session fields, `kafka_partition`, `kafka_offset`, `spark_batch_id`, `preprocessing_status` | Eventu válidu ne'ebé Spark kometadu ho trace Kafka no batch | Schema/query DB no total rows |
| SQLite `spark_batches` | `batch_id` (PK), `received`, `valid`, `invalid`, `duplicates`, `processing_seconds`, `processed_at` | Resumu unidade micro-batch ida | Log Spark no batch record pareadu |

Output JSON no SQLite rai iha `artifacts/`; ficheiru CSV raw nafatin iha `data/`. `received`, `valid`, `invalid`, `duplicates`, producer ACK no offset reprezenta unidade diferente, entaun relatóriu tenke hatudu sira separadu no esplika diferensa.

## Apéndise C — Protokolu Verifikasaun Pipeline

Ba capture ida, rai evidénsia tuir etapa: (1) kontajen rejistu ne'ebé estudante husu; (2) total replay producer ne'ebé hetan ACK; (3) partition/offset Kafka ne'ebé observa; (4) batch Spark ho total received/valid/invalid/duplicate; (5) output JSON no SQLite rows ho `event_id`; no (6) screenshot dashboard no tempu captura. Ba kada etapa, nota data, hora, konfigurasaun replay, taxa eventu, limit no status final. Relata timeout/erro no diferensa count; labele foti count hosi etapa ida hanesan count etapa seluk.

| Metrika | Unidade | Fonte observasaun |
|---|---|---|
| Eventu ne'ebé husu | rejistu/eventu |
| Producer publish ACK | mensajen ne'ebé broker simu |
| Kafka | offset iha partition |
| Spark `received/valid/invalid/duplicates` | eventu ka event ID tuir agregasaun implementada |
| SQLite `processed_events` | event ID úniku ne'ebé kometadu |
| Durasaun batch | segundu; hatudu konfigurasaun teste |

## Apéndise D — Evidénsia Dashboard

Hatama screenshot ne'ebé estudante captura hosi aplikasaun rasik: dashboard Overview; Event Stream durante replay; Purchase Funnel; Product Analytics tuir `item_id`; No-Purchase Analysis ho denominador sesaun elegível; Pipeline Monitoring; Data Quality; no status bainhira replay para. Kada imajen tenke iha legenda, data/ora, capture ID no nota kona-ba pipeline ativa/idle. Screenshot tenke la revela informasaun ne'ebé dataset la iha, hanesan click event ka naran produtu.

## Apéndise E — Instrusaun Demonstrasaun Lokal

1. Prepara Docker Desktop no ambiente Python/projetu tuir README implementasaun.
2. Iha pasta projetu, hahú dependénsia: `docker compose up -d --build`.
3. Konfirma servisu ho `docker compose ps`; verifica Kafka no Spark logs se status la di'ak.
4. Hahú dashboard Streamlit tuir komandus ne'ebé dokumenta iha README no asesu URL lokál ne'ebé aplikasaun hatudu.
5. Hili capture/replay ho limite eventu no taxa ne'ebé apropriadu ba laptop; regista konfigurasaun no capture ID.
6. Tuir eventu ida hosi payload/ACK Kafka to'o Spark batch no SQLite, depois salva screenshot no log ba Apéndise C no D.
7. Bainhira demo remata, nota status final no para kontainer tuir prosedimentu README. La aprezenta dataset istóriku replay hanesan tráfiku loja live.

## Apéndise F — Regra Interpretasaun Analítika

View, add to cart no transaction mak event count, la'ós ema úniku. Sesaun hahú foun bainhira gap entre eventu visitante hanesan boot liu 30 minutu; sesaun elegível tenke iha view ida ka add to cart ida. “Laiha transasaun rejistada” signifika de'it la iha eventu transaction iha sesaun ne'e. Click la disponivel iha dataset ne'e. `item_id` la hanesan naran produtu. Kualker interpretasaun dashboard tenke tuir limitasaun hirak-ne'e.

---

**Nota kona-ba lian:** Naran teknolojia, koluna dataset, tipu eventu, no identificadór iha kódigu hela iha forma orijinál atu nune'e bele kompara diretamente ho sistema no fonte dadus. Esplikasaun no estrutura proposta nian hakerek iha Tetun.

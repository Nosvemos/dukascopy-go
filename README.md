# dukascopy-data-downloader

Go ile yazilmis, Dukascopy `jetta` API uzerinden enstruman arayip tarihsel veri indirebilen bir CLI.

## Ozellikler

- `xauusd`, `eur/usd`, `BTC-USD` gibi farkli sembol yazimlarini cozer
- `instruments` komutuyla enstruman arar
- `download` komutuyla `tick`, `minute`, `hour`, `day` verisini CSV olarak indirir
- Go icinde mock API uzerinden calisan e2e testler barindirir

## Kurulum

```bash
go build -o dukascopy-data ./cmd/dukascopy
```

## Kullanim

Enstruman arama:

```bash
go run ./cmd/dukascopy instruments --query xauusd
```

Dakikalik veri indirme:

```bash
go run ./cmd/dukascopy download \
  --symbol xauusd \
  --granularity minute \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T06:00:00Z \
  --output ./data/xauusd-minute.csv
```

Tick veri indirme:

```bash
go run ./cmd/dukascopy download \
  --symbol eurusd \
  --granularity tick \
  --from 2024-01-02T00:00:00Z \
  --to 2024-01-02T01:00:00Z \
  --output ./data/eurusd-ticks.csv
```

## Konfigurasyon

Varsayilan API tabani:

```text
https://jetta.dukascopy.com
```

Istersen `DUKASCOPY_API_BASE_URL` ortam degiskeniyle override edebilirsin. Bu ozellikle testlerde kullaniliyor.

## Test

Tum testler:

```bash
go test ./...
```

E2E testleri:

```bash
go test ./e2e -v
```

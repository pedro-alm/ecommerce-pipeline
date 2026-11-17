"""Gera um CSV sintético no MESMO formato do dataset do Kaggle (carrie1/ecommerce-data).

Serve para testar o pipeline sem baixar o dataset real e como plano B na apresentação.
Reproduz de propósito os problemas de qualidade previstos na N2: duplicados, linhas sem
CustomerID, sem descrição, preço zerado, cancelamentos (prefixo "C"), códigos de serviço,
espaços extras e codificação Latin-1.

Uso:  python scripts/gerar_amostra.py [--faturas 3000]
"""
import argparse
import csv
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import config  # noqa: E402

PRODUTOS = [
    ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 2.55),
    ("71053", "WHITE METAL LANTERN", 3.39),
    ("84406B", "CREAM CUPID HEARTS COAT HANGER", 2.75),
    ("84029G", "KNITTED UNION FLAG HOT WATER BOTTLE", 3.39),
    ("22752", "SET 7 BABUSHKA NESTING BOXES", 7.65),
    ("21730", "GLASS STAR FROSTED T-LIGHT HOLDER", 4.25),
    ("22633", "HAND WARMER UNION JACK", 1.85),
    ("22632", "HAND WARMER RED POLKA DOT", 1.85),
    ("84879", "ASSORTED COLOUR BIRD ORNAMENT", 1.69),
    ("22745", "POPPY'S PLAYHOUSE BEDROOM", 2.10),
    ("22748", "POPPY'S PLAYHOUSE KITCHEN", 2.10),
    ("22310", "IVORY KNITTED MUG COSY", 1.65),
    ("21071", "VINTAGE BILLBOARD DRINK ME MUG", 1.25),
    ("22960", "JAM MAKING SET WITH JARS", 4.25),
    ("22086", "PAPER CHAIN KIT 50'S CHRISTMAS", 2.55),
    ("23084", "RABBIT NIGHT LIGHT", 2.08),
    ("21212", "PACK OF 72 RETROSPOT CAKE CASES", 0.55),
    ("84991", "60 TEATIME FAIRY CAKE CASES", 0.55),
    ("22423", "REGENCY CAKESTAND 3 TIER", 12.75),
    ("47566", "PARTY BUNTING", 4.95),
    ("85099B", "JUMBO BAG RED RETROSPOT", 2.08),
    ("20725", "LUNCH BAG RED RETROSPOT", 1.65),
    ("22197", "SMALL POPCORN HOLDER", 0.85),
    ("23166", "MEDIUM CERAMIC TOP STORAGE JAR", 1.04),
    ("22178", "VICTORIAN GLASS HANGING T-LIGHT", 2.55),
    ("22111", "SCOTTIE DOG HOT WATER BOTTLE", 4.95),
    ("21931", "JUMBO STORAGE BAG SUKI", 2.08),
    ("22386", "JUMBO BAG PINK POLKADOT", 2.08),
    ("22720", "SET OF 3 CAKE TINS PANTRY DESIGN", 4.95),
    ("23203", "JUMBO BAG DOILEY PATTERNS", 2.08),
    ("22469", "HEART OF WICKER SMALL", 1.65),
    ("22111B", "CAFÉ COASTER SET OF 6", 3.75),
    ("22910", "PAPER CHAIN KIT VINTAGE CHRISTMAS", 2.95),
    ("23298", "SPOTTY BUNTING", 4.95),
    ("22457", "NATURAL SLATE HEART CHALKBOARD", 2.95),
]
SERVICOS = [("POST", "POSTAGE", 18.00), ("D", "Discount", 12.50), ("M", "Manual", 25.00),
            ("BANK CHARGES", "Bank Charges", 15.00)]
PAISES = ["France", "Germany", "EIRE", "Netherlands", "Australia", "Spain", "Belgium",
          "Switzerland", "Norway", "RSA", "USA"]


def gerar(n_faturas: int, destino: Path, seed: int = 42) -> int:
    rnd = random.Random(seed)
    clientes = {}
    for cid in range(12346, 12346 + 400):
        clientes[cid] = "United Kingdom" if rnd.random() < 0.88 else rnd.choice(PAISES)
    ids = list(clientes)

    inicio = datetime(2010, 12, 1, 8, 26)
    fim = datetime(2011, 12, 9, 12, 50)
    total_min = int((fim - inicio).total_seconds() // 60)

    # Mais vendas no fim do ano (sazonalidade)
    def sorteia_data():
        while True:
            d = inicio + timedelta(minutes=rnd.randint(0, total_min))
            if not 8 <= d.hour <= 19:  # horário comercial, como no dataset real
                continue
            peso = 1.0 + (1.2 if d.month in (10, 11) else 0) + (0.4 if d.month == 12 and d.year == 2010 else 0)
            if rnd.random() < peso / 2.6:
                return d

    datas = sorted(sorteia_data() for _ in range(n_faturas))
    linhas, vendidos, numero = [], [], 536365

    def fmt(d):  # formato do Kaggle: 12/1/2010 8:26 (sem zero à esquerda)
        return f"{d.month}/{d.day}/{d.year} {d.hour}:{d.minute:02d}"

    for d in datas:
        cid = rnd.choice(ids)
        pais = clientes[cid]
        sem_cliente = rnd.random() < 0.10
        cliente_csv = "" if sem_cliente else str(cid)
        if sem_cliente and rnd.random() < 0.5:
            pais = "United Kingdom"

        if rnd.random() < 0.035 and vendidos:  # cancelamento / devolução
            for cod, desc, preco in rnd.sample(vendidos, k=min(len(vendidos), rnd.randint(1, 3))):
                linhas.append([f"C{numero}", cod, desc, -rnd.randint(1, 12), fmt(d), f"{preco:.2f}", cliente_csv, pais])
            numero += 1
            continue

        for cod, desc, preco in rnd.sample(PRODUTOS, k=rnd.randint(1, 9)):
            qtd = rnd.choice([1, 2, 3, 4, 6, 6, 8, 12, 12, 24, 48])
            desc_csv = desc
            if rnd.random() < 0.04:
                desc_csv = f"  {desc.replace(' ', '  ', 1)} "  # espaços extras
            linhas.append([str(numero), cod, desc_csv, qtd, fmt(d), f"{preco:.2f}", cliente_csv, pais])
            vendidos.append((cod, desc, preco))
        if rnd.random() < 0.15:
            cod, desc, preco = rnd.choice(SERVICOS)
            linhas.append([str(numero), cod, desc, 1, fmt(d), f"{preco:.2f}", cliente_csv, pais])
        if rnd.random() < 0.02:  # ajuste de estoque: sem descrição, preço zero, sem cliente
            linhas.append([str(numero), rnd.choice(PRODUTOS)[0], "", -rnd.randint(1, 20), fmt(d), "0.00", "", "United Kingdom"])
        numero += 1

    # ~1% de linhas duplicadas
    for linha in rnd.sample(linhas, k=max(1, len(linhas) // 100)):
        linhas.append(list(linha))
    rnd.shuffle(linhas)
    linhas.sort(key=lambda l: datetime.strptime(l[4], "%m/%d/%Y %H:%M"))

    destino.parent.mkdir(parents=True, exist_ok=True)
    with open(destino, "w", newline="", encoding=config.CSV_ENCODING) as f:
        w = csv.writer(f)
        w.writerow(["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
                    "UnitPrice", "CustomerID", "Country"])
        w.writerows(linhas)
    return len(linhas)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--faturas", type=int, default=3000)
    ap.add_argument("--destino", type=Path, default=config.SAMPLE_FILE)
    args = ap.parse_args()
    n = gerar(args.faturas, args.destino)
    print(f"Amostra sintética gerada: {n} linhas em {args.destino}")

# scripts/atualizar_repasses.py - gera data/repasses_*.csv (roda no GitHub Actions, de madrugada)
# Coleta os convenios e contratos de repasse de todas as UFs (SC primeiro), avalia os pontos de atencao e grava os
# resumos. Uso: TRANSPARENCIA_API_KEY=... python scripts/atualizar_repasses.py [UF ...]
# Regras de seguranca: se SC falhar, nada e gravado e o job sai com 1; UF que falha ou nao cabe no tempo fica fora
# do comparativo (coluna "completa" = False), nunca aparece como "sem repasse".
# As outras UFs rodam em 3 threads (a API aguenta ~300 requisicoes por minuto de madrugada).
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import dou  # noqa: E402
import repasses  # noqa: E402
from dou import ErroColeta  # noqa: E402

LIMITE_MIN = float(os.getenv("LIMITE_MINUTOS", "120"))
THREADS = int(os.getenv("THREADS", "3"))


def coletar(uf: str, chave: str, t0: float):
    """(uf, DataFrame|None, erro|None). Nao comeca UF nova depois do limite de tempo."""
    if time.time() - t0 > LIMITE_MIN * 60:
        return uf, None, "tempo esgotado"
    try:
        itens = repasses.coletar_uf(uf, chave, session=requests.Session())
        print(f"{uf}: {len(itens)} instrumentos ({(time.time() - t0) / 60:.1f} min)", flush=True)
        return uf, repasses.normalizar(itens, uf), None
    except ErroColeta as e:
        print(f"{uf}: FALHOU ({e})", file=sys.stderr, flush=True)
        return uf, None, str(e)


def main(argv) -> int:
    chave = os.getenv("TRANSPARENCIA_API_KEY", "")
    if not chave:
        print("Defina TRANSPARENCIA_API_KEY.", file=sys.stderr)
        return 1
    ufs = [u.upper() for u in argv] or [repasses.UF_DESTAQUE] + [u for u in repasses.UFS if u != repasses.UF_DESTAQUE]
    t0 = time.time()
    resultados = []
    if repasses.UF_DESTAQUE in ufs:  # SC primeiro e sozinha: se falhar, nem seguimos
        resultados.append(coletar(repasses.UF_DESTAQUE, chave, t0))
        if resultados[0][1] is None:
            print("SC não foi coletada: nada gravado, arquivos anteriores mantidos.", file=sys.stderr)
            return 1
    resto = [u for u in ufs if u != repasses.UF_DESTAQUE]
    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        resultados += list(ex.map(lambda u: coletar(u, chave, t0), resto))
    quadros = [df for _, df, _ in resultados if df is not None]
    completas = [uf for uf, df, _ in resultados if df is not None]
    falhas = [f"{uf}: {e}" for uf, df, e in resultados if df is None]
    if any("chave" in f for f in falhas):
        print("Chave da API recusada: nada gravado.", file=sys.stderr)
        return 1
    d = repasses.avaliar(pd.concat(quadros, ignore_index=True))
    resumos = repasses.resumir(d, completas)
    ini, _ = repasses.janela()
    repasses.salvar(resumos, agora=dou.agora_brasilia().strftime("%d/%m/%Y %H:%M"), ufs_completas=completas,
                    janela_txt=f"vigência terminando de {ini} em diante")
    print(f"gravado: {len(d)} instrumentos, {len(completas)}/{len(ufs)} UFs completas, {len(falhas)} falhas, "
          f"{(time.time() - t0) / 60:.0f} min")
    for f in falhas:
        print("  -", f, file=sys.stderr)
    return 1 if falhas else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

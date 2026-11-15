# scripts/atualizar_repasses.py - gera data/repasses_*.csv (roda no GitHub Actions, de madrugada)
# Coleta os convenios e contratos de repasse de todas as UFs (SC primeiro), avalia os pontos de atencao e grava os
# resumos. Uso: TRANSPARENCIA_API_KEY=... python scripts/atualizar_repasses.py [UF ...]
# Regras de seguranca: se SC falhar, nada e gravado e o job sai com 1; UF que falha fica fora do comparativo
# (coluna "completa" = False), nunca aparece como "sem repasse".
import os
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import dou  # noqa: E402
import repasses  # noqa: E402
from dou import ErroColeta  # noqa: E402

LIMITE_MIN = float(os.getenv("LIMITE_MINUTOS", "110"))


def main(argv) -> int:
    chave = os.getenv("TRANSPARENCIA_API_KEY", "")
    if not chave:
        print("Defina TRANSPARENCIA_API_KEY.", file=sys.stderr)
        return 1
    ufs = [u.upper() for u in argv] or [repasses.UF_DESTAQUE] + [u for u in repasses.UFS if u != repasses.UF_DESTAQUE]
    t0 = time.time()
    quadros, completas, falhas = [], [], []
    for uf in ufs:
        if time.time() - t0 > LIMITE_MIN * 60:
            falhas.append(f"{uf}: tempo esgotado")
            continue
        try:
            itens = repasses.coletar_uf(uf, chave)
            quadros.append(repasses.normalizar(itens, uf))
            completas.append(uf)
            print(f"{uf}: {len(itens)} instrumentos ({(time.time() - t0) / 60:.1f} min)", flush=True)
        except ErroColeta as e:
            falhas.append(f"{uf}: {e}")
            print(f"{uf}: FALHOU ({e})", file=sys.stderr, flush=True)
            if "chave" in str(e):
                break
    if repasses.UF_DESTAQUE not in completas:
        print("SC não foi coletada: nada gravado, arquivos anteriores mantidos.", file=sys.stderr)
        return 1
    d = repasses.avaliar(pd.concat(quadros, ignore_index=True))
    resumos = repasses.resumir(d, completas)
    ini, fim = repasses.janela()
    repasses.salvar(resumos, agora=dou.agora_brasilia().strftime("%d/%m/%Y %H:%M"), ufs_completas=completas,
                    janela_txt=f"vigência terminando de {ini} em diante")
    print(f"gravado: {len(d)} instrumentos, {len(completas)}/{len(ufs)} UFs completas, {len(falhas)} falhas")
    for f in falhas:
        print("  -", f, file=sys.stderr)
    return 1 if falhas else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

# scripts/atualizar_cargos_viajantes.py - gera data/viagens_cargos.csv (cargo e funcao dos viajantes)
# Cruza os viajantes dos resumos (data/viagens_AAAA_viajantes.csv) com o cadastro mensal de servidores da CGU
# (SIAPE 83 MB + Banco Central 0,26 MB), por nome completo + 6 digitos visiveis do CPF. Roda no GitHub Actions.
# Nao baixa de novo se o mes do cadastro e o mesmo e nenhum viajante novo apareceu.
# Se algo falhar, mantem o arquivo anterior e sai com codigo 1.
import glob
import os
import sys
import tempfile
import zipfile

import pandas as pd
import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import servidores  # noqa: E402

MIN_BYTES = {"siape": 20_000_000, "bacen": 50_000}
LIMITE_CSV = 1_500_000_000


def viajantes(pasta: str) -> pd.DataFrame:
    quadros = [pd.read_csv(f, dtype=str, keep_default_na=False, usecols=["cpf", "nome"])
               for f in sorted(glob.glob(os.path.join(pasta, "viagens_*_viajantes.csv")))]
    if not quadros:
        return pd.DataFrame(columns=["cpf", "nome"])
    v = pd.concat(quadros).drop_duplicates()
    return v[v["cpf"].str.contains(r"\*", na=False) & ~v["nome"].str.contains("sigilo", case=False)]


def baixar_cadastro(ym: str, fonte: str, destino: str) -> str:
    """Baixa o zip, extrai so o CSV do cadastro e devolve o caminho dele."""
    url = f"{servidores.BASE_URL}/{servidores.FONTES[fonte].format(ym=ym)}"
    zip_path = os.path.join(destino, f"{fonte}.zip")
    tam = 0
    with requests.get(url, headers=servidores.HEADERS, stream=True, timeout=(30, 900)) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for bloco in r.iter_content(1 << 20):
                f.write(bloco)
                tam += len(bloco)
    if tam < MIN_BYTES[fonte]:
        raise RuntimeError(f"{fonte}: arquivo pequeno demais ({tam} bytes)")
    alvo = f"{ym}_Cadastro.csv"
    with zipfile.ZipFile(zip_path) as z:
        info = next((i for i in z.infolist() if os.path.basename(i.filename) == alvo), None)
        if info is None or info.file_size > LIMITE_CSV:
            raise RuntimeError(f"{fonte}: {alvo} não encontrado ou grande demais")
        info.filename = f"{fonte}_{alvo}"  # sem caminho dentro do zip
        caminho = z.extract(info, destino)
    os.remove(zip_path)
    return caminho


def main() -> int:
    pasta = servidores.PASTA_DADOS
    v = viajantes(pasta)
    if v.empty:
        print("Sem viajantes nos resumos; nada a fazer.")
        return 0
    ym = servidores.mes_mais_recente()
    if not ym:
        print("Nenhum mês do cadastro encontrado; arquivo anterior mantido.", file=sys.stderr)
        return 1
    chaves = {servidores.chave(n, c) for n, c in zip(v["nome"], v["cpf"])}
    atual = servidores.ler(pasta)
    if atual is not None:
        tabela_atual, meta = atual
        if meta.get("referencia") == ym and chaves.issubset(set(tabela_atual["chave"])):
            print(f"Cadastro de {ym} já aplicado e sem viajantes novos: nada a baixar.")
            return 0
    try:
        with tempfile.TemporaryDirectory() as tmp:
            quadros = []
            for fonte in ("siape", "bacen"):
                caminho = baixar_cadastro(ym, fonte, tmp)
                quadros.append(servidores.ler_cadastro(caminho, fonte, chaves))
                os.remove(caminho)
    except (requests.RequestException, RuntimeError, ValueError, zipfile.BadZipFile) as e:
        print(f"Falhou, arquivo anterior mantido: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    cad = pd.concat([q for q in quadros if not q.empty], ignore_index=True) if any(not q.empty for q in quadros) else None
    if cad is None:
        print("Nenhum viajante localizado: formato do cadastro mudou? Arquivo anterior mantido.", file=sys.stderr)
        return 1
    tabela = servidores.montar_tabela(v, servidores.consolidar(cad, ym), ym)
    achou = float((tabela["status"] == "localizado").mean())
    if achou < 0.2:  # trava de seguranca: abaixo disso algo mudou no formato (a taxa normal fica perto de 65%)
        print(f"Só {achou:.0%} localizados: suspeito de mudança de formato. Arquivo anterior mantido.", file=sys.stderr)
        return 1
    servidores.salvar(tabela, ym, pasta)
    print(f"{ym}: {len(tabela)} viajantes, {int((tabela['status'] == 'localizado').sum())} localizados ({achou:.0%})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

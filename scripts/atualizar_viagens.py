# scripts/atualizar_viagens.py - gera os resumos data/viagens_AAAA_*.csv (roda no GitHub Actions)
# Baixa o arquivo "Viagens a Servico" da CGU (125 a 157 MB por ano), calcula os resumos e grava em data/.
# Uso: python scripts/atualizar_viagens.py [ano ...]
# Padrao: o ano atual (sempre refaz) e o anterior (so se ainda nao existir). Se algo falhar, mantem o
# arquivo anterior e sai com codigo 1: falha nunca vira "viagem sem gasto".
import os
import shutil
import sys
import tempfile
import zipfile

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import dou  # noqa: E402
import viagens  # noqa: E402

URL = "https://portaldatransparencia.gov.br/download-de-dados/viagens/{ano}"
MIN_BYTES = 20_000_000  # abaixo disso o zip esta quebrado


def baixar(ano: int, destino: str) -> str:
    """Baixa o zip (segue o redirecionamento) e devolve o endereco final, que traz a data da carga."""
    with requests.get(URL.format(ano=ano), stream=True, timeout=(30, 600), allow_redirects=True,
                      headers={"User-Agent": "Mozilla/5.0 (FiscalizaGov)"}) as r:
        r.raise_for_status()
        tam = 0
        with open(os.path.join(destino, "arq.zip"), "wb") as f:
            for bloco in r.iter_content(1 << 20):
                f.write(bloco)
                tam += len(bloco)
        if tam < MIN_BYTES:
            raise RuntimeError(f"arquivo de {ano} pequeno demais ({tam} bytes)")
        return r.url


def processar_ano(ano: int, agora: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        fonte = baixar(ano, tmp)
        pasta = os.path.join(tmp, str(ano))
        with zipfile.ZipFile(os.path.join(tmp, "arq.zip")) as z:
            esperados = {f"{ano}_Viagem.csv", f"{ano}_Trecho.csv", f"{ano}_Passagem.csv"}
            nomes = {os.path.basename(i.filename) for i in z.infolist()}
            if not esperados.issubset(nomes):
                raise RuntimeError(f"zip de {ano} sem os arquivos esperados: faltam {esperados - nomes}")
            for i in z.infolist():
                if os.path.basename(i.filename) in esperados:
                    i.filename = os.path.basename(i.filename)  # evita caminhos dentro do zip
                    z.extract(i, pasta)
        os.remove(os.path.join(tmp, "arq.zip"))
        resumos = viagens.processar(pasta, ano)
        if resumos["orgao"].empty:
            raise RuntimeError(f"{ano}: nenhum registro processado")
        viagens.salvar(resumos, ano, agora=agora, fonte=fonte)
        print(f"{ano}: {int(resumos['orgao']['viagens'].sum())} viagens resumidas ({fonte})")
        shutil.rmtree(pasta, ignore_errors=True)


def main(argv) -> int:
    ano_atual = dou.hoje_brasilia().year
    if argv:
        anos = [int(a) for a in argv]
    else:
        anos = [ano_atual]
        if not os.path.exists(os.path.join(viagens.PASTA_DADOS, f"viagens_{ano_atual - 1}_orgao.csv")):
            anos.append(ano_atual - 1)
    agora = dou.agora_brasilia().strftime("%d/%m/%Y %H:%M")
    falhou = False
    for ano in anos:
        try:
            processar_ano(ano, agora)
        except Exception as e:  # noqa: BLE001 - qualquer falha mantem o arquivo anterior
            print(f"{ano}: falhou, arquivo anterior mantido ({type(e).__name__}: {e})", file=sys.stderr)
            falhou = True
    return 1 if falhou else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

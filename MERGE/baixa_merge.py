import httpx
import asyncio
import pandas as pd
from pathlib import Path

url_base = 'https://ftp.cptec.inpe.br/modelos/tempo/MERGE/GPM/HOURLY'
diretorio = Path("MERGE", "horario")
limitador_tarefas = asyncio.Semaphore(7)

diretorio.mkdir(parents=True, exist_ok=True)

inicio = "2019-08-13"
fim = "2023-12-31"

datas = pd.date_range(start=inicio, end=fim, freq="H").tolist()

async def obter_merge(data: pd.Timestamp) -> None:
    """
    Baixa MERGE.

    Parameters
    ----------
    data : pd.Timestamp
        Timestamp
    """
    async with limitador_tarefas:
        async with httpx.AsyncClient() as cliente:
            arquivo = f'MERGE_CPTEC_{data.year}{data.strftime("%m")}{data.strftime("%d")}{data.strftime('%H')}.grib2'
            url_requisicao = (
                    f"{url_base}/{data.year}/{data.strftime("%m")}/{data.strftime("%d")}/{arquivo}"
                )
                
            resposta = await cliente.get(url_requisicao, timeout=30)
            await asyncio.sleep(2)

            if resposta.status_code == 200:
                conteudo = resposta.content
                diretorio_saida = Path(diretorio, arquivo)

                with open(diretorio_saida, "wb") as arquivo_:
                    arquivo_.write(conteudo)
                        
                print(f"{arquivo} [ok]")

            else:
                print(f"Não foi possível obter o {arquivo}.")
                print(url_requisicao)

async def main():
    tarefas = [asyncio.create_task(obter_merge(data)) for data in datas]
    await asyncio.gather(*tarefas)

if __name__ == "__main__":
    asyncio.run(main())
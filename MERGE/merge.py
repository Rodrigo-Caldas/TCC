import xarray as xr
import pandas as pd 
import rioxarray
import rasterio
from pathlib import Path
import geopandas as gpd
from tqdm import tqdm


def extract_data_utc(nome_arquivo: Path) -> pd.Timestamp:
    """Extrai datetime UTC do nome do arquivo."""
    nome = nome_arquivo.stem  # MERGE_CPTEC_YYYYMMDDHH
    return pd.to_datetime(nome.split("_")[-1], format="%Y%m%d%H")


# Caminho da pasta
pasta = Path("horario")

# Lista de arquivos ordenados
arquivos = sorted(pasta.glob("MERGE_CPTEC_*.grib2"))

arquivos_por_mes = {}
for arq in arquivos:
    data = extract_data_utc(arq)
    chave = (data.year, data.month)
    arquivos_por_mes.setdefault(chave, []).append((data, arq))

# Lista de datasets
datasets = []

print("Lendo arquivos GRIB2 e convertendo para xarray...")

# Loop por mês
for (ano, mes), entradas in sorted(arquivos_por_mes.items()):
    print(f"\n🗓️ Processando {ano}-{mes:02d} ({len(entradas)} arquivos)...")

    datasets = []

    for data_utc, arq in tqdm(entradas, desc=f"{ano}-{mes:02d}"):
        try:
            ds = xr.open_dataset(arq, engine="cfgrib", decode_timedelta=False)
            ds_prec = ds[["prec"]]  # variável confirmada como 'prec'
            ds_prec = ds_prec.expand_dims(time=[data_utc])
            datasets.append(ds_prec)

        except Exception as e:
            print(f"⚠️ Erro em {arq.name}: {e}")

    if not datasets:
        print(f"⚠️ Nenhum dado válido para {ano}-{mes:02d}, pulando...")
        continue

    # Concatena todos em um só dataset horário
    ds_horario = xr.concat(datasets, dim="time")
    # Corrige o tempo para horário de Brasília (UTC-3)
    ds_brasilia = ds_horario.assign_coords(time=ds_horario.time - pd.Timedelta(hours=3))
    # Acumula a precipitação diária
    ds_diario = ds_brasilia.resample(time="1D").sum()

    # Salva em NetCDF
    saida = f"prec_diaria_brasilia_{ano}_{mes:02d}.nc"
    ds_diario.to_netcdf(saida)
    print(f"✅ Arquivo salvo: {saida}")

print("✅ Concluído! Precipitação diária gerada.")
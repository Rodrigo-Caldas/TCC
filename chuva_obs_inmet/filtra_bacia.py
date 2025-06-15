import geopandas as gpd
import pandas as pd

from shapely.geometry import Point
from pathlib import Path
from typing import Dict

def extrair_lat_lon(caminho_csv: Path) -> Dict[str, float]:
    """
    Extrai Latitude e Longitude do arquivo csv.

    Parameters
    ----------
    caminho_csv : Path
        Caminho do arquivo csv.

    Returns
    -------
    dict[str, float]
        Dicionário contendo latitude e longitude.
    """
    try:
        with open(caminho_csv, encoding="latin1") as f:
            linhas = [next(f).strip() for _ in range(8)]

        metadados = {}

        for linha in linhas:
            if ";" in linha:
                chave, valor = linha.split(";", 1)
                metadados[chave.strip()] = valor.strip()

        latitude = float(metadados.get("LATITUDE:", "nan").replace(",", "."))
        longitude = float(metadados.get("LONGITUDE:", "nan").replace(",", "."))
        codigo = str(metadados.get("CODIGO (WMO):", "nan"))

        dict = {
            "latitude": latitude,
            "longitude": longitude,
            "codigo": codigo
            }

        return dict
    
    except Exception as erro:
        print(f"Não foi possível obter lat e lon de {caminho_csv}.")
        print(erro)


def filtrar_postos_da_bacia(
    df: pd.DataFrame, shp: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Filtra os postos do inventário a partir do contorno shapefile desejado.

    Parameters
    ----------
    df : pd.Dataframe
        Dataframe do inventário ANA, vinda da função obter_inventario().
    shp : gpd.GeoDataFrame
        Contorno desejado.

    Returns
    -------
    gpd.GeoDataFrame
        Postos que estão dentro do contorno.
    """
    df_inventario = df.copy()

    geometria = [
        Point(xy) for xy in zip(df_inventario["longitude"], df_inventario["latitude"])
    ]
    gdf_inventario = gpd.GeoDataFrame(
        df_inventario, geometry=geometria, crs="EPSG:4326"
    )

    if shp.crs != gdf_inventario.crs:
        shp = shp.set_crs(gdf_inventario.crs, allow_override=True)

    gdf_inventario = gdf_inventario[
        gdf_inventario.geometry.within(shp.geometry.union_all())
    ]

    return gdf_inventario

caminho_csvs = Path("/home/rodrigocaldas/dados_tcc/chuva_obs_inmet/dados-organizados")
caminho_bacias = Path("/home/rodrigocaldas/dados_tcc/chuva_obs_inmet/sub-bacias-isoladas")
dados_filtrados = Path("dados-filtrados")

caminhos_csvs = list(caminho_csvs.rglob("*.CSV"))
caminhos_bacias = list(caminho_bacias.rglob("*.shp"))

for caminho_bacia in caminhos_bacias:
    for caminho_csv in caminhos_csvs:
        lat_lon_cod = extrair_lat_lon(caminho_csv=caminho_csv)

        gdf = gpd.read_file(caminho_bacia)
        df = pd.read_csv(caminho_csv, sep=';', skiprows=8, encoding='latin1')
        df = df.drop(df.columns[3::], axis=1)

        df["latitude"] = lat_lon_cod["latitude"]
        df["longitude"] = lat_lon_cod["longitude"]
        df["codigo"] = lat_lon_cod["codigo"]

        gdf_filtrado = filtrar_postos_da_bacia(df, gdf)

        if not gdf_filtrado.shape[0] > 0 or gdf_filtrado["Data"].isnull().all():
            pass

        else:
            caminho_dados_filtrados = Path(dados_filtrados, caminho_bacia.name)
            caminho_dados_filtrados.mkdir(parents=True, exist_ok=True)
            gdf_filtrado.to_csv(f"{caminho_dados_filtrados}/{caminho_csv.name}")
            #caminho_csv.unlink()

            print(f"Estação {caminho_csv.name} pertence a bacia {caminho_bacia.name}!")

print("Fim do serviço!")

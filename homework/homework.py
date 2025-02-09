"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

from zipfile import ZipFile
import os
import pandas as pd

def clean_campaign_data():
    month_to_number = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }

    ruta = os.path.join("files", "input")
    zips = [nombre for nombre in os.listdir(ruta)]
    dataframes = []
    
    for zip in zips:
        open_zip = os.path.join(ruta, zip)
        with ZipFile(open_zip) as zip_ref:
            with zip_ref.open(zip_ref.namelist()[0]) as file:
                df = pd.read_csv(file)
                if "Unnamed: 0" in df.columns:
                    df = df.drop(columns=["Unnamed: 0"])
                dataframes.append(df)

    final_df = pd.concat(dataframes, ignore_index=True)

    client = final_df[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]]
    client["job"] = client["job"].str.replace(".", "").str.replace("-", "_")
    client["education"] = client["education"].replace("unknown", pd.NA)
    client["education"] = client["education"].apply(lambda x: x.replace("-", "_") if pd.notna(x) else x)
    client["education"] = client["education"].apply(lambda x: x.replace(".", "_") if pd.notna(x) else x)
    client["credit_default"] = client["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
    client["mortgage"] = client["mortgage"].apply(lambda x: 1 if x == "yes" else 0)

    campaign = final_df[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "month", "day"]]
    campaign["month"] = campaign["month"].apply(lambda x: month_to_number.get(x.lower(), "00"))
    campaign["previous_outcome"] = campaign["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
    campaign["campaign_outcome"] = campaign["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
    
    campaign.loc[:, "month"] = campaign["month"].astype(str).str.zfill(2)  
    campaign.loc[:, "day"] = campaign["day"].astype(str).str.zfill(2)  
    
    campaign.loc[:, "last_contact_date"] = "2022-" + campaign["month"] + "-" + campaign["day"]
    campaign = campaign.drop(['month', 'day'], axis=1)

    economic = final_df[["client_id", "cons_price_idx", "euribor_three_months"]]

    output_path = os.path.join("files", "output")
    if not os.path.isdir(output_path):
        os.makedirs(output_path)

    client.to_csv(os.path.join(output_path, "client.csv"), index=False)
    campaign.to_csv(os.path.join(output_path, "campaign.csv"), index=False)
    economic.to_csv(os.path.join(output_path, "economics.csv"), index=False)

    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    return


if __name__ == "__main__":
    clean_campaign_data()

import os
import yaml

from azure.storage.blob import ContainerClient

def load_config():
    dir_root = os.path.dirname(os.path.abspath(__file__))
    with open(dir_root + "/config.yaml", "r") as yamlfile:
        return yaml.load(yamlfile, Loader=yaml.FullLoader)

def get_files(dir):
    with os.scandir(dir) as entries:
        for entry in entries:
            if entry.is_file() and not entry.name.startswith('.'):
                yield entry

def upload(files, connection_string, container_name):
    container_client = ContainerClient.from_connection_string(connection_string, container_name)
    
    #print("Uploading files to blob storage")

    for file in files:
        blob_client = container_client.get_blob_client(file.name)
        if blob_client.exists():
            continue
            #print("File already exists")
        else:
            with open(file.path, "rb") as data:
                blob_client.upload_blob(data)
                #print(f'{file.name} uploaded to blob storage')
    
def download_files(connection_string, container_name):
    container_client = ContainerClient.from_connection_string(connection_string, container_name)
    my_blobs = container_client.list_blobs()
    for blob in my_blobs:
        blob_bytes = container_client.get_blob_client(blob).download_blob().readall()
        save_blob(blob.name, blob_bytes, container_name)

def save_blob(file_name, file_content, container_name):
    download_file_path = os.path.join(config["download_folder"]+'/'+container_name, file_name)
    os.makedirs(os.path.dirname(download_file_path), exist_ok=True)
    with open(download_file_path, "wb") as file:
        file.write(file_content)

config = load_config()
videos = get_files(config["source_folder"]+"/videos")
pictures = get_files(config["source_folder"]+"/pictures")

upload(videos, config["azure_storage_connectionstring"], config["videos_container_name"])
upload(pictures, config["azure_storage_connectionstring"], config["pictures_container_name"])

download_files(config["azure_storage_connectionstring"], config["pictures_container_name"])
download_files(config["azure_storage_connectionstring"], config["videos_container_name"])

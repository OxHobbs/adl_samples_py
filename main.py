import os
from azure.core.exceptions import ResourceExistsError
from azure.core.paging import ItemPaged
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient, DataLakeDirectoryClient
from config.storageaccount import STORAGE_ACCOUNT_INFO
import glob


# global initializers
service_client: DataLakeServiceClient
file_system_client: FileSystemClient
directory_client: DataLakeDirectoryClient


def initialize_storage_account(sa_name: str, sa_key: str) -> None:
    try:
        global service_client
        service_client = DataLakeServiceClient(account_url=f"https://{sa_name}.dfs.core.windows.net",
                                               credential=sa_key)
    except Exception as e:
        print(e)


def create_file_system(file_system_name: str) -> None:
    try:
        global file_system_client
        file_system_client = service_client.create_file_system(file_system=file_system_name)
    except ResourceExistsError as e:
        print(f"File System: {file_system_name} already exists")
        file_system_client = service_client.get_file_system_client(file_system_name)
    except Exception as e:
        print(e)


def create_directory(directory: str) -> None:
    try:
        global file_system_client
        global directory_client
        directory_client = file_system_client.create_directory(directory)
    except ResourceExistsError as e:
        print(f"Directory: {directory} already exists")
    except Exception as e:
        print(e)


def upload_file_to_directory(file_path: str) -> None:
    try:
        global file_system_client
        global directory_client

        _, file_name = os.path.split(file_path)
        file_client = directory_client.create_file(file_name)
        with open(file_path, 'rb') as fin:
            file_contents = fin.read()

        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))
    except Exception as e:
        print(e)


def list_directory_contents(directory: str) -> None:
    try:
        global file_system_client
        # file_system_client = service_client.get_file_system_client(file_system="my-file-system")
        paths = file_system_client.get_paths(path=directory)
        for path in paths:
            print(path.name + '\n')
    except Exception as e:
        print(e)


def get_directory_contents(directory: str) -> ItemPaged:
    try:
        global file_system_client
        return file_system_client.get_paths(path=directory)
    except Exception as e:
        print(e)


def get_files_to_upload(directory: str) -> list:
    return glob.glob(f"{directory}/*gz")


def get_dir_from_filename(file_name: str) -> str:
    year, month, day = file_name[4:8], file_name[8:10], file_name[10:12]
    return f"{year}/{month}/{day}"


def main() -> None:
    sa_name = os.environ.get('STORAGE_ACCOUNT_NAME') or STORAGE_ACCOUNT_INFO.name
    sa_key = os.environ.get('STORAGE_ACCOUNT_KEY') or STORAGE_ACCOUNT_INFO.key

    file_system = 'myfs'
    source_directory = os.path.join(os.environ.get('USERPROFILE'), 'Downloads', 'gzipFiles')

    initialize_storage_account(sa_name, sa_key)
    create_file_system(file_system)

    files_to_upload = get_files_to_upload(source_directory)
    for file in files_to_upload:
        file_base, file_name = os.path.split(file)
        directory = get_dir_from_filename(file_name)
        print(f"Creating directory at: {directory}")
        create_directory(directory)

        directory_contents = get_directory_contents(directory)
        file_already_exists = False
        for path in directory_contents:
            existing_file_name = path.name.split('/')[-1]
            if file_name == existing_file_name:
                file_already_exists = True
        if file_already_exists:
            print(f"{file_name} already exists in ADL")
            continue
        upload_file_to_directory(file)
        list_directory_contents(directory)


if __name__ == '__main__':
    main()

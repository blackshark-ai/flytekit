import os
import tempfile
from datetime import timedelta

import mock
import pytest
import yaml

from flytekit import AzurePersistence
from flytekit.configuration import AzureBlobConfig, DataConfig, ConfigFile
from flytekit.extras.persistence.abfs_azurecli import STORAGE_ACCOUNT_NAME_ENV_VAR, STORAGE_ACCOUNT_KEY_ENV_VAR


@mock.patch("flytekit.extras.persistence.abfs_azurecli.AzurePersistence._check_binary")
def test_does_not_work_without_config(mock_check):
    proxy = AzurePersistence()
    with pytest.raises(ValueError):
        proxy.exists("abfs://test/fdsa/fdsa")


@mock.patch("flytekit.extras.persistence.abfs_azurecli.AzurePersistence._check_binary")
@mock.patch("flytekit.extras.persistence.abfs_azurecli.subprocess")
def test_works_with_config(mock_subprocess, mock_check):
    proxy = AzurePersistence(
        data_config=DataConfig(
            azure=AzureBlobConfig(
                storage_account_name="foo",
                storage_account_key="bar",
                backoff=timedelta(seconds=0),
                retries=10
            )
        )
    )
    proxy.exists("abfs://test/fdsa/fdsa")


@mock.patch("flytekit.extras.persistence.abfs_azurecli.AzurePersistence._check_binary")
@mock.patch("flytekit.extras.persistence.abfs_azurecli.subprocess")
def test_works_with_set_env_vars(mock_subprocess, mock_check):
    proxy = AzurePersistence()
    os.environ[STORAGE_ACCOUNT_NAME_ENV_VAR] = "foo"
    os.environ[STORAGE_ACCOUNT_KEY_ENV_VAR] = "bar"
    proxy.exists("abfs://test/fdsa/fdsa")


@mock.patch("flytekit.extras.persistence.abfs_azurecli.AzurePersistence._check_binary")
@mock.patch("flytekit.extras.persistence.abfs_azurecli.subprocess")
def test_works_with_config_file(mock_subprocess, mock_check):
    with tempfile.NamedTemporaryFile(suffix='.yaml', mode='w+') as flytekit_config_file:
        dict_config = {
            "storage": {
                "connection": {
                    "storage-account-name": "foo",
                    "storage-account-key": "foo",
                }
            }
        }
        yaml.dump(dict_config, flytekit_config_file)
        azure_cfg = AzureBlobConfig.auto(config_file=ConfigFile(location=flytekit_config_file.name))
        proxy = AzurePersistence(data_config=DataConfig(azure=azure_cfg))
        proxy.exists("abfs://test/fdsa/fdsa")


@mock.patch("flytekit.extras.persistence.abfs_azurecli.AzurePersistence._check_binary")
@mock.patch("flytekit.extras.persistence.abfs_azurecli.subprocess")
def test_retries(mock_subprocess, mock_check):
    mock_subprocess.check_call.side_effect = Exception("ErrorCode:ContainerNotFound")
    mock_check.return_value = True

    proxy = AzurePersistence(
        data_config=DataConfig(
            azure=AzureBlobConfig(
                storage_account_name="foo",
                storage_account_key="bar",
                backoff=timedelta(seconds=0),
                retries=10
            )
        )
    )
    assert proxy.exists("abfs://test/fdsa/fdsa") is False
    assert mock_subprocess.check_call.call_count == 11


@mock.patch("flytekit.extras.persistence.abfs_azurecli._update_cmd_config_and_execute")
@mock.patch("flytekit.extras.persistence.abfs_azurecli.AzurePersistence._check_binary")
def test_put(mock_check, mock_exec):
    os.environ['AZURE_STORAGE_ACCOUNT'] = "az-storage-account"
    proxy = AzurePersistence()
    proxy.put("/test", "abfs://az-blob-container/k1")
    mock_exec.assert_called_with(
        azure_cfg=AzureBlobConfig.auto(),
        cmd=[
            "az", "storage", "copy",
            "--source", "/test",
            "--destination", "https://az-storage-account.blob.core.windows.net/az-blob-container/k1"
        ]
    )


@mock.patch("flytekit.extras.persistence.abfs_azurecli._update_cmd_config_and_execute")
@mock.patch("flytekit.extras.persistence.abfs_azurecli.AzurePersistence._check_binary")
def test_put_recursive(mock_check, mock_exec):
    os.environ['AZURE_STORAGE_ACCOUNT'] = "az-storage-account"
    proxy = AzurePersistence()
    proxy.put("/test", "abfs://az-blob-container/k1", True)
    mock_exec.assert_called_with(
        azure_cfg=AzureBlobConfig.auto(),
        cmd=[
            "az", "storage", "copy",
            "--source", "/test/*",
            "--destination", "https://az-storage-account.blob.core.windows.net/az-blob-container/k1",
            "--recursive"
        ]
    )


@mock.patch("flytekit.extras.persistence.abfs_azurecli._update_cmd_config_and_execute")
@mock.patch("flytekit.extras.persistence.abfs_azurecli.AzurePersistence._check_binary")
def test_get(mock_check, mock_exec):
    os.environ['AZURE_STORAGE_ACCOUNT'] = "az-storage-account"
    proxy = AzurePersistence()
    proxy.get("abfs://az-blob-container/k1", "/test")
    mock_exec.assert_called_with(
        azure_cfg=AzureBlobConfig.auto(),
        cmd=[
            'az', 'storage', 'copy',
            '--source', 'https://az-storage-account.blob.core.windows.net/az-blob-container/k1',
            '--destination', '/test'
        ]
    )


@mock.patch("flytekit.extras.persistence.abfs_azurecli._update_cmd_config_and_execute")
@mock.patch("flytekit.extras.persistence.abfs_azurecli.AzurePersistence._check_binary")
def test_get_recursive(mock_check, mock_exec):
    os.environ['AZURE_STORAGE_ACCOUNT'] = "az-storage-account"
    proxy = AzurePersistence()
    proxy.get("abfs://az-blob-container/k1", "/test", True)
    mock_exec.assert_called_with(
        azure_cfg=AzureBlobConfig.auto(),
        cmd=[
            'az', 'storage', 'copy',
            '--source', 'https://az-storage-account.blob.core.windows.net/az-blob-container/k1',
            '--destination', '/test',
            '--recursive'
        ],
    )


def test_construct_path():
    proxy = AzurePersistence()
    p = proxy.construct_path(True, False, "xyz")
    assert p == "abfs://xyz"

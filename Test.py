

def test_load_config_success():
    valid_yaml_content = """
    dev:
        VAULT_ROLE: 02f424c0-3b07-40cb-b6c0-73a732204e133
        LOCKBOX_ID: 5c9969ea-0b05-467c-8dde-09995ea5c70f
    """
    expected_dict = {
        'dev': {
            'VAULT_ROLE': '02f424c0-3b07-40cb-b6c0-73a732204e133',
            'LOCKBOX_ID': '5c9969ea-0b05-467c-8dde-09995ea5c70f'
        }
    }

    m = mock_open(read_data=valid_yaml_content)

    with patch("builtins.open", m, create=True):
        with patch("os.path.join", return_value="/fake/path/app_config.yaml"):
            with patch("os.path.abspath", return_value="/fake/path"):
                with patch("yaml.safe_load", return_value=expected_dict) as mock_yaml:
                    config = config_read.load_config("dev")
                    print("Loaded config:", config)
                    mock_yaml.assert_called_once_with(valid_yaml_content)
                    assert config is not None
                    assert 'dev' in config
                    assert config['dev']['VAULT_ROLE'] == '02f424c0-3b07-40cb-b6c0-73a732204e133'

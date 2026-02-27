CREATE TABLE `my-project-8584-jetonai.decom.ods_android_download` (
    date DATE,
    package_name STRING,
    country STRING,
    daily_device_installs INT,
    daily_device_uninstalls INT,
    daily_device_upgrades INT,
    total_user_installs INT,
    daily_user_installs INT,
    daily_user_uninstalls INT,
    active_device_installs INT,
    install_events INT,
    update_events INT,
    uninstall_events INT
)
PARTITION BY
    date
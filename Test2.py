
#!/bin/bash

echo "Cleaning RAM..."
sudo purge

echo "Cleaning DNS cache..."
sudo dscacheutil -flushcache
sudo killall -HUP mDNSResponder

echo "Cleaning user cache..."
rm -rf ~/Library/Caches/*

echo "Cleaning system logs..."
sudo rm -rf /private/var/log/*
sudo rm -rf /Library/Logs/*

echo "Cleaning old iOS backups..."
rm -rf ~/Library/Application\ Support/MobileSync/Backup/*

echo "Cleaning trash..."
rm -rf ~/.Trash/*

echo "Cleaning temp folders..."
sudo rm -rf /private/var/tmp/*
sudo rm -rf /private/var/folders/*

echo "Done! Restart your Mac for best results."

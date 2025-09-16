# -*- mode: python -*-

block_cipher = None


a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[('chromedriver.exe', '.')],
    hiddenimports=[
        'Scripts.direct_dw_films_scraper',
        'Scripts.direct_dw_series_scraper',
        'Scripts.update_movies_premiere',
        'Scripts.update_movies_updated',
        'Scripts.update_episodes_premiere',
        'Scripts.update_episodes_updated',
        'Scripts.torrent_dw_films_scraper',
        'Scripts.torrent_dw_series_scraper',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='HdfullScrappers',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    icon='resources/ico.ico',
)
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='HdfullScrappers',
)


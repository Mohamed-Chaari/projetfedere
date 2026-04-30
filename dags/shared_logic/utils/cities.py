"""
cities.py — Comprehensive registry of ALL Tunisian cities and delegations,
organised by governorate (wilaya) and region.

Regions:
  • Nord       — Grand Tunis + northern governorates
  • Nord-Est   — Cap Bon & Sahel nord
  • Nord-Ouest — North-west interior
  • Centre     — Central coast + interior
  • Centre-Ouest — Central-west interior
  • Sud-Est    — South-east coast
  • Sud-Ouest  — South-west interior

Every governorate capital is included, plus all major delegations and
notable towns — 100+ locations for full national coverage.
"""

# ──────────────────────────────────────────────────────────────
#  COMPLETE TUNISIA CITIES REGISTRY — 24 GOVERNORATES
# ──────────────────────────────────────────────────────────────

TUNISIA_CITIES = [
    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE TUNIS  (Nord)
    # ═══════════════════════════════════════════════════════════
    {"name": "Tunis",              "lat": 36.8065, "lon": 10.1815, "governorate": "Tunis",          "region": "Nord"},
    {"name": "La Marsa",           "lat": 36.8783, "lon": 10.3253, "governorate": "Tunis",          "region": "Nord"},
    {"name": "Le Bardo",           "lat": 36.8092, "lon": 10.1346, "governorate": "Tunis",          "region": "Nord"},
    {"name": "Le Kram",            "lat": 36.8333, "lon": 10.3167, "governorate": "Tunis",          "region": "Nord"},
    {"name": "Carthage",           "lat": 36.8528, "lon": 10.3233, "governorate": "Tunis",          "region": "Nord"},
    {"name": "Sidi Bou Said",      "lat": 36.8687, "lon": 10.3417, "governorate": "Tunis",          "region": "Nord"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE L'ARIANA  (Nord)
    # ═══════════════════════════════════════════════════════════
    {"name": "Ariana",             "lat": 36.8625, "lon": 10.1956, "governorate": "Ariana",         "region": "Nord"},
    {"name": "La Soukra",          "lat": 36.8531, "lon": 10.2114, "governorate": "Ariana",         "region": "Nord"},
    {"name": "Raoued",             "lat": 36.9069, "lon": 10.1897, "governorate": "Ariana",         "region": "Nord"},
    {"name": "Sidi Thabet",        "lat": 36.9108, "lon": 10.1200, "governorate": "Ariana",         "region": "Nord"},
    {"name": "Kalaat el-Andalous", "lat": 36.9833, "lon": 10.1000, "governorate": "Ariana",         "region": "Nord"},
    {"name": "Mnihla",             "lat": 36.8333, "lon": 10.2000, "governorate": "Ariana",         "region": "Nord"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE BEN AROUS  (Nord)
    # ═══════════════════════════════════════════════════════════
    {"name": "Ben Arous",          "lat": 36.7531, "lon": 10.2189, "governorate": "Ben Arous",      "region": "Nord"},
    {"name": "Hammam Lif",         "lat": 36.7333, "lon": 10.3333, "governorate": "Ben Arous",      "region": "Nord"},
    {"name": "Hammam Chatt",       "lat": 36.7500, "lon": 10.3333, "governorate": "Ben Arous",      "region": "Nord"},
    {"name": "Ezzahra",            "lat": 36.7439, "lon": 10.3067, "governorate": "Ben Arous",      "region": "Nord"},
    {"name": "Rades",              "lat": 36.7667, "lon": 10.2833, "governorate": "Ben Arous",      "region": "Nord"},
    {"name": "Megrine",            "lat": 36.7667, "lon": 10.2333, "governorate": "Ben Arous",      "region": "Nord"},
    {"name": "Mohamedia",          "lat": 36.6667, "lon": 10.1833, "governorate": "Ben Arous",      "region": "Nord"},
    {"name": "Fouchana",           "lat": 36.7000, "lon": 10.1667, "governorate": "Ben Arous",      "region": "Nord"},
    {"name": "Mornag",             "lat": 36.6833, "lon": 10.2833, "governorate": "Ben Arous",      "region": "Nord"},
    {"name": "Bou Mhel el-Bassatine", "lat": 36.7333, "lon": 10.2333, "governorate": "Ben Arous",  "region": "Nord"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE LA MANOUBA  (Nord)
    # ═══════════════════════════════════════════════════════════
    {"name": "Manouba",            "lat": 36.8078, "lon": 10.0975, "governorate": "Manouba",        "region": "Nord"},
    {"name": "Den Den",            "lat": 36.8200, "lon": 10.0900, "governorate": "Manouba",        "region": "Nord"},
    {"name": "Douar Hicher",       "lat": 36.7833, "lon": 10.0833, "governorate": "Manouba",        "region": "Nord"},
    {"name": "Oued Ellil",         "lat": 36.8167, "lon": 10.0500, "governorate": "Manouba",        "region": "Nord"},
    {"name": "Tebourba",           "lat": 36.8333, "lon":  9.8500, "governorate": "Manouba",        "region": "Nord"},
    {"name": "Jedaida",            "lat": 36.8500, "lon":  9.9333, "governorate": "Manouba",        "region": "Nord"},
    {"name": "Borj El Amri",       "lat": 36.7167, "lon":  9.9000, "governorate": "Manouba",        "region": "Nord"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE NABEUL  (Nord-Est / Cap Bon)
    # ═══════════════════════════════════════════════════════════
    {"name": "Nabeul",             "lat": 36.4513, "lon": 10.7357, "governorate": "Nabeul",         "region": "Nord-Est"},
    {"name": "Hammamet",           "lat": 36.4000, "lon": 10.6167, "governorate": "Nabeul",         "region": "Nord-Est"},
    {"name": "Korba",              "lat": 36.5833, "lon": 10.8667, "governorate": "Nabeul",         "region": "Nord-Est"},
    {"name": "Menzel Temime",      "lat": 36.7833, "lon": 10.9833, "governorate": "Nabeul",         "region": "Nord-Est"},
    {"name": "Kelibia",            "lat": 36.8500, "lon": 11.1000, "governorate": "Nabeul",         "region": "Nord-Est"},
    {"name": "Grombalia",          "lat": 36.6000, "lon": 10.5000, "governorate": "Nabeul",         "region": "Nord-Est"},
    {"name": "Soliman",            "lat": 36.6833, "lon": 10.5000, "governorate": "Nabeul",         "region": "Nord-Est"},
    {"name": "Beni Khiar",         "lat": 36.4667, "lon": 10.8000, "governorate": "Nabeul",         "region": "Nord-Est"},
    {"name": "Dar Chaabane",       "lat": 36.4667, "lon": 10.7500, "governorate": "Nabeul",         "region": "Nord-Est"},
    {"name": "El Haouaria",        "lat": 37.0500, "lon": 11.0167, "governorate": "Nabeul",         "region": "Nord-Est"},
    {"name": "Menzel Bouzelfa",    "lat": 36.6667, "lon": 10.5833, "governorate": "Nabeul",         "region": "Nord-Est"},
    {"name": "Bou Argoub",         "lat": 36.5333, "lon": 10.5500, "governorate": "Nabeul",         "region": "Nord-Est"},
    {"name": "Takelsa",            "lat": 36.7833, "lon": 10.8500, "governorate": "Nabeul",         "region": "Nord-Est"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE BIZERTE  (Nord)
    # ═══════════════════════════════════════════════════════════
    {"name": "Bizerte",            "lat": 37.2744, "lon":  9.8739, "governorate": "Bizerte",        "region": "Nord"},
    {"name": "Menzel Bourguiba",   "lat": 37.1500, "lon":  9.7833, "governorate": "Bizerte",        "region": "Nord"},
    {"name": "Menzel Jemil",       "lat": 37.2333, "lon":  9.9167, "governorate": "Bizerte",        "region": "Nord"},
    {"name": "Mateur",             "lat": 37.0403, "lon":  9.6653, "governorate": "Bizerte",        "region": "Nord"},
    {"name": "Ras Jebel",          "lat": 37.2167, "lon": 10.1167, "governorate": "Bizerte",        "region": "Nord"},
    {"name": "Sejnane",            "lat": 37.0583, "lon":  9.2361, "governorate": "Bizerte",        "region": "Nord"},
    {"name": "Tinja",              "lat": 37.1667, "lon":  9.7500, "governorate": "Bizerte",        "region": "Nord"},
    {"name": "Utique",             "lat": 37.0583, "lon":  9.8333, "governorate": "Bizerte",        "region": "Nord"},
    {"name": "Ghar El Melh",       "lat": 37.1667, "lon": 10.1833, "governorate": "Bizerte",        "region": "Nord"},
    {"name": "Joumine",            "lat": 36.9167, "lon":  9.4167, "governorate": "Bizerte",        "region": "Nord"},
    {"name": "Ghezala",            "lat": 36.9833, "lon":  9.5500, "governorate": "Bizerte",        "region": "Nord"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE BEJA  (Nord-Ouest)
    # ═══════════════════════════════════════════════════════════
    {"name": "Beja",               "lat": 36.7256, "lon":  9.1817, "governorate": "Beja",           "region": "Nord-Ouest"},
    {"name": "Medjez el-Bab",      "lat": 36.6500, "lon":  9.6167, "governorate": "Beja",           "region": "Nord-Ouest"},
    {"name": "Nefza",              "lat": 36.9667, "lon":  9.0833, "governorate": "Beja",           "region": "Nord-Ouest"},
    {"name": "Testour",            "lat": 36.5500, "lon":  9.4500, "governorate": "Beja",           "region": "Nord-Ouest"},
    {"name": "Teboursouk",         "lat": 36.4500, "lon":  9.2500, "governorate": "Beja",           "region": "Nord-Ouest"},
    {"name": "Goubellat",          "lat": 36.5333, "lon":  9.6833, "governorate": "Beja",           "region": "Nord-Ouest"},
    {"name": "Amdoun",             "lat": 36.7833, "lon":  9.0667, "governorate": "Beja",           "region": "Nord-Ouest"},
    {"name": "Thibar",             "lat": 36.5167, "lon":  9.1167, "governorate": "Beja",           "region": "Nord-Ouest"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE JENDOUBA  (Nord-Ouest)
    # ═══════════════════════════════════════════════════════════
    {"name": "Jendouba",           "lat": 36.5011, "lon":  8.7803, "governorate": "Jendouba",       "region": "Nord-Ouest"},
    {"name": "Tabarka",            "lat": 36.9544, "lon":  8.7578, "governorate": "Jendouba",       "region": "Nord-Ouest"},
    {"name": "Ain Draham",         "lat": 36.7833, "lon":  8.6833, "governorate": "Jendouba",       "region": "Nord-Ouest"},
    {"name": "Bou Salem",          "lat": 36.6167, "lon":  8.9667, "governorate": "Jendouba",       "region": "Nord-Ouest"},
    {"name": "Ghardimaou",         "lat": 36.4500, "lon":  8.4333, "governorate": "Jendouba",       "region": "Nord-Ouest"},
    {"name": "Fernana",            "lat": 36.6556, "lon":  8.6956, "governorate": "Jendouba",       "region": "Nord-Ouest"},
    {"name": "Oued Mliz",          "lat": 36.5667, "lon":  8.5833, "governorate": "Jendouba",       "region": "Nord-Ouest"},
    {"name": "Balta-Bou Aouane",   "lat": 36.5833, "lon":  8.8167, "governorate": "Jendouba",       "region": "Nord-Ouest"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DU KEF  (Nord-Ouest)
    # ═══════════════════════════════════════════════════════════
    {"name": "Le Kef",             "lat": 36.1742, "lon":  8.7047, "governorate": "Kef",            "region": "Nord-Ouest"},
    {"name": "Dahmani",            "lat": 35.9500, "lon":  8.8333, "governorate": "Kef",            "region": "Nord-Ouest"},
    {"name": "Tajerouine",         "lat": 35.8917, "lon":  8.5500, "governorate": "Kef",            "region": "Nord-Ouest"},
    {"name": "Sakiet Sidi Youssef","lat": 36.2333, "lon":  8.3500, "governorate": "Kef",            "region": "Nord-Ouest"},
    {"name": "El Ksour",           "lat": 35.9167, "lon":  8.7667, "governorate": "Kef",            "region": "Nord-Ouest"},
    {"name": "Kalaat Snan",        "lat": 35.8000, "lon":  8.4500, "governorate": "Kef",            "region": "Nord-Ouest"},
    {"name": "Jerissa",            "lat": 35.8500, "lon":  8.6333, "governorate": "Kef",            "region": "Nord-Ouest"},
    {"name": "Kalaat Khasba",      "lat": 35.8167, "lon":  8.7500, "governorate": "Kef",            "region": "Nord-Ouest"},
    {"name": "Nebeur",             "lat": 36.1000, "lon":  8.7167, "governorate": "Kef",            "region": "Nord-Ouest"},
    {"name": "Sers",               "lat": 36.0833, "lon":  9.0167, "governorate": "Kef",            "region": "Nord-Ouest"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE SILIANA  (Nord-Ouest)
    # ═══════════════════════════════════════════════════════════
    {"name": "Siliana",            "lat": 36.0847, "lon":  9.3706, "governorate": "Siliana",        "region": "Nord-Ouest"},
    {"name": "Bou Arada",          "lat": 36.3500, "lon":  9.6167, "governorate": "Siliana",        "region": "Nord-Ouest"},
    {"name": "Gaafour",            "lat": 36.3167, "lon":  9.3167, "governorate": "Siliana",        "region": "Nord-Ouest"},
    {"name": "Makthar",            "lat": 35.8500, "lon":  9.2000, "governorate": "Siliana",        "region": "Nord-Ouest"},
    {"name": "Rouhia",             "lat": 35.9500, "lon":  9.0667, "governorate": "Siliana",        "region": "Nord-Ouest"},
    {"name": "Kesra",              "lat": 35.8167, "lon":  9.3667, "governorate": "Siliana",        "region": "Nord-Ouest"},
    {"name": "El Krib",            "lat": 36.3333, "lon":  9.1833, "governorate": "Siliana",        "region": "Nord-Ouest"},
    {"name": "Bargou",             "lat": 36.0833, "lon":  9.5333, "governorate": "Siliana",        "region": "Nord-Ouest"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE ZAGHOUAN  (Nord)
    # ═══════════════════════════════════════════════════════════
    {"name": "Zaghouan",           "lat": 36.4028, "lon": 10.1431, "governorate": "Zaghouan",       "region": "Nord"},
    {"name": "El Fahs",            "lat": 36.3667, "lon":  9.9000, "governorate": "Zaghouan",       "region": "Nord"},
    {"name": "Nadhour",            "lat": 36.5000, "lon": 10.0667, "governorate": "Zaghouan",       "region": "Nord"},
    {"name": "Bir Mcherga",        "lat": 36.5167, "lon":  9.8667, "governorate": "Zaghouan",       "region": "Nord"},
    {"name": "Zriba",              "lat": 36.3333, "lon": 10.0833, "governorate": "Zaghouan",       "region": "Nord"},
    {"name": "Saouaf",             "lat": 36.2500, "lon": 10.0833, "governorate": "Zaghouan",       "region": "Nord"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE SOUSSE  (Centre)
    # ═══════════════════════════════════════════════════════════
    {"name": "Sousse",             "lat": 35.8288, "lon": 10.6405, "governorate": "Sousse",         "region": "Centre"},
    {"name": "Msaken",             "lat": 35.7333, "lon": 10.5833, "governorate": "Sousse",         "region": "Centre"},
    {"name": "Kalaa Kebira",       "lat": 35.8667, "lon": 10.5333, "governorate": "Sousse",         "region": "Centre"},
    {"name": "Kalaa Sghira",       "lat": 35.8333, "lon": 10.6000, "governorate": "Sousse",         "region": "Centre"},
    {"name": "Hammam Sousse",      "lat": 35.8667, "lon": 10.6000, "governorate": "Sousse",         "region": "Centre"},
    {"name": "Akouda",             "lat": 35.8667, "lon": 10.5667, "governorate": "Sousse",         "region": "Centre"},
    {"name": "Enfidha",            "lat": 36.1333, "lon": 10.3833, "governorate": "Sousse",         "region": "Centre"},
    {"name": "Sidi Bou Ali",       "lat": 35.9667, "lon": 10.4667, "governorate": "Sousse",         "region": "Centre"},
    {"name": "Hergla",             "lat": 36.0333, "lon": 10.5000, "governorate": "Sousse",         "region": "Centre"},
    {"name": "Kondar",             "lat": 35.9000, "lon": 10.5667, "governorate": "Sousse",         "region": "Centre"},
    {"name": "Sidi El Heni",       "lat": 35.6667, "lon": 10.3167, "governorate": "Sousse",         "region": "Centre"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE MONASTIR  (Centre)
    # ═══════════════════════════════════════════════════════════
    {"name": "Monastir",           "lat": 35.7833, "lon": 10.8333, "governorate": "Monastir",       "region": "Centre"},
    {"name": "Moknine",            "lat": 35.6333, "lon": 10.9000, "governorate": "Monastir",       "region": "Centre"},
    {"name": "Jemmal",             "lat": 35.6167, "lon": 10.7500, "governorate": "Monastir",       "region": "Centre"},
    {"name": "Ksar Hellal",        "lat": 35.6500, "lon": 10.9000, "governorate": "Monastir",       "region": "Centre"},
    {"name": "Teboulba",           "lat": 35.6667, "lon": 10.9667, "governorate": "Monastir",       "region": "Centre"},
    {"name": "Sahline",            "lat": 35.7500, "lon": 10.7167, "governorate": "Monastir",       "region": "Centre"},
    {"name": "Ksibet el-Mediouni", "lat": 35.6833, "lon": 10.8500, "governorate": "Monastir",       "region": "Centre"},
    {"name": "Bekalta",            "lat": 35.6167, "lon": 11.0000, "governorate": "Monastir",       "region": "Centre"},
    {"name": "Bembla",             "lat": 35.6500, "lon": 10.7833, "governorate": "Monastir",       "region": "Centre"},
    {"name": "Sayada",             "lat": 35.6667, "lon": 10.8833, "governorate": "Monastir",       "region": "Centre"},
    {"name": "Ouerdanine",         "lat": 35.7000, "lon": 10.6500, "governorate": "Monastir",       "region": "Centre"},
    {"name": "Zeramdine",          "lat": 35.5667, "lon": 10.7333, "governorate": "Monastir",       "region": "Centre"},
    {"name": "Beni Hassen",        "lat": 35.5500, "lon": 10.8167, "governorate": "Monastir",       "region": "Centre"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE MAHDIA  (Centre)
    # ═══════════════════════════════════════════════════════════
    {"name": "Mahdia",             "lat": 35.5047, "lon": 11.0622, "governorate": "Mahdia",         "region": "Centre"},
    {"name": "El Jem",             "lat": 35.2975, "lon": 10.7117, "governorate": "Mahdia",         "region": "Centre"},
    {"name": "Ksour Essef",        "lat": 35.4167, "lon": 10.9833, "governorate": "Mahdia",         "region": "Centre"},
    {"name": "Chebba",             "lat": 35.2333, "lon": 11.1167, "governorate": "Mahdia",         "region": "Centre"},
    {"name": "Bou Merdes",         "lat": 35.4500, "lon": 10.7333, "governorate": "Mahdia",         "region": "Centre"},
    {"name": "Sidi Alouane",       "lat": 35.3833, "lon": 10.9667, "governorate": "Mahdia",         "region": "Centre"},
    {"name": "Melloulech",         "lat": 35.1500, "lon": 11.0333, "governorate": "Mahdia",         "region": "Centre"},
    {"name": "Chorbane",           "lat": 35.2833, "lon": 10.3833, "governorate": "Mahdia",         "region": "Centre"},
    {"name": "Souassi",            "lat": 35.3500, "lon": 10.5500, "governorate": "Mahdia",         "region": "Centre"},
    {"name": "Essouassi",          "lat": 35.3500, "lon": 10.5333, "governorate": "Mahdia",         "region": "Centre"},
    {"name": "Hebira",             "lat": 35.4333, "lon": 10.8833, "governorate": "Mahdia",         "region": "Centre"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE SFAX  (Centre)
    # ═══════════════════════════════════════════════════════════
    {"name": "Sfax",               "lat": 34.7406, "lon": 10.7603, "governorate": "Sfax",           "region": "Centre"},
    {"name": "Sakiet Ezzit",       "lat": 34.7833, "lon": 10.7500, "governorate": "Sfax",           "region": "Centre"},
    {"name": "Sakiet Eddaier",     "lat": 34.7167, "lon": 10.7833, "governorate": "Sfax",           "region": "Centre"},
    {"name": "Chihia",             "lat": 34.7500, "lon": 10.7333, "governorate": "Sfax",           "region": "Centre"},
    {"name": "Thyna",              "lat": 34.7100, "lon": 10.7400, "governorate": "Sfax",           "region": "Centre"},
    {"name": "El Ain",             "lat": 34.7300, "lon": 10.6800, "governorate": "Sfax",           "region": "Centre"},
    {"name": "Gremda",             "lat": 34.7200, "lon": 10.7000, "governorate": "Sfax",           "region": "Centre"},
    {"name": "Agareb",             "lat": 34.7500, "lon": 10.4667, "governorate": "Sfax",           "region": "Centre"},
    {"name": "Jebeniana",          "lat": 34.9833, "lon": 10.4500, "governorate": "Sfax",           "region": "Centre"},
    {"name": "El Hencha",          "lat": 34.9167, "lon": 10.5667, "governorate": "Sfax",           "region": "Centre"},
    {"name": "Menzel Chaker",      "lat": 34.8833, "lon": 10.3833, "governorate": "Sfax",           "region": "Centre"},
    {"name": "Bir Ali Ben Khalifa","lat": 34.5000, "lon": 10.0833, "governorate": "Sfax",           "region": "Centre"},
    {"name": "Skhira",             "lat": 34.3000, "lon": 10.0667, "governorate": "Sfax",           "region": "Centre"},
    {"name": "Mahares",            "lat": 34.5333, "lon": 10.5000, "governorate": "Sfax",           "region": "Centre"},
    {"name": "Kerkennah",          "lat": 34.7000, "lon": 11.1667, "governorate": "Sfax",           "region": "Centre"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE KAIROUAN  (Centre-Ouest)
    # ═══════════════════════════════════════════════════════════
    {"name": "Kairouan",           "lat": 35.6744, "lon": 10.0963, "governorate": "Kairouan",       "region": "Centre-Ouest"},
    {"name": "Sbikha",             "lat": 35.9333, "lon": 10.0167, "governorate": "Kairouan",       "region": "Centre-Ouest"},
    {"name": "Haffouz",            "lat": 35.6333, "lon":  9.6833, "governorate": "Kairouan",       "region": "Centre-Ouest"},
    {"name": "Nasrallah",          "lat": 35.3333, "lon":  9.8500, "governorate": "Kairouan",       "region": "Centre-Ouest"},
    {"name": "Hajeb El Ayoun",     "lat": 35.3833, "lon":  9.5500, "governorate": "Kairouan",       "region": "Centre-Ouest"},
    {"name": "Bou Hajla",          "lat": 35.5000, "lon": 10.1667, "governorate": "Kairouan",       "region": "Centre-Ouest"},
    {"name": "Oueslatia",          "lat": 35.8500, "lon":  9.6000, "governorate": "Kairouan",       "region": "Centre-Ouest"},
    {"name": "Chebika (Kairouan)", "lat": 35.5500, "lon":  9.9000, "governorate": "Kairouan",       "region": "Centre-Ouest"},
    {"name": "El Ala",             "lat": 35.6167, "lon":  9.5500, "governorate": "Kairouan",       "region": "Centre-Ouest"},
    {"name": "Echrarda",           "lat": 35.5667, "lon": 10.1833, "governorate": "Kairouan",       "region": "Centre-Ouest"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE KASSERINE  (Centre-Ouest)
    # ═══════════════════════════════════════════════════════════
    {"name": "Kasserine",          "lat": 35.1678, "lon":  8.8306, "governorate": "Kasserine",      "region": "Centre-Ouest"},
    {"name": "Sbeitla",            "lat": 35.2167, "lon":  9.1333, "governorate": "Kasserine",      "region": "Centre-Ouest"},
    {"name": "Thala",              "lat": 35.5667, "lon":  8.6667, "governorate": "Kasserine",      "region": "Centre-Ouest"},
    {"name": "Feriana",            "lat": 34.9500, "lon":  8.5833, "governorate": "Kasserine",      "region": "Centre-Ouest"},
    {"name": "Foussana",           "lat": 34.9333, "lon":  8.3500, "governorate": "Kasserine",      "region": "Centre-Ouest"},
    {"name": "Sbiba",              "lat": 35.5333, "lon":  9.0833, "governorate": "Kasserine",      "region": "Centre-Ouest"},
    {"name": "Majel Bel Abbes",    "lat": 34.8833, "lon":  8.6500, "governorate": "Kasserine",      "region": "Centre-Ouest"},
    {"name": "Hassi El Ferid",     "lat": 34.8333, "lon":  8.3167, "governorate": "Kasserine",      "region": "Centre-Ouest"},
    {"name": "Jedeliane",          "lat": 35.3167, "lon":  9.3000, "governorate": "Kasserine",      "region": "Centre-Ouest"},
    {"name": "Ezzouhour",          "lat": 35.1833, "lon":  8.8167, "governorate": "Kasserine",      "region": "Centre-Ouest"},
    {"name": "El Ayoun",           "lat": 35.1000, "lon":  9.2500, "governorate": "Kasserine",      "region": "Centre-Ouest"},
    {"name": "Hidra",              "lat": 35.0667, "lon":  8.6167, "governorate": "Kasserine",      "region": "Centre-Ouest"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE SIDI BOUZID  (Centre-Ouest)
    # ═══════════════════════════════════════════════════════════
    {"name": "Sidi Bouzid",        "lat": 35.0381, "lon":  9.4847, "governorate": "Sidi Bouzid",    "region": "Centre-Ouest"},
    {"name": "Regueb",             "lat": 34.8500, "lon":  9.7833, "governorate": "Sidi Bouzid",    "region": "Centre-Ouest"},
    {"name": "Jilma",              "lat": 34.8833, "lon":  9.3833, "governorate": "Sidi Bouzid",    "region": "Centre-Ouest"},
    {"name": "Menzel Bouzaiane",   "lat": 34.5833, "lon":  9.7500, "governorate": "Sidi Bouzid",    "region": "Centre-Ouest"},
    {"name": "Bir El Hafey",       "lat": 34.9333, "lon":  9.1167, "governorate": "Sidi Bouzid",    "region": "Centre-Ouest"},
    {"name": "Meknassy",           "lat": 34.6167, "lon":  9.6000, "governorate": "Sidi Bouzid",    "region": "Centre-Ouest"},
    {"name": "Sidi Ali Ben Aoun",  "lat": 35.0500, "lon":  9.2333, "governorate": "Sidi Bouzid",    "region": "Centre-Ouest"},
    {"name": "Mezzouna",           "lat": 34.4833, "lon":  9.7833, "governorate": "Sidi Bouzid",    "region": "Centre-Ouest"},
    {"name": "Ouled Haffouz",      "lat": 35.0667, "lon":  9.6833, "governorate": "Sidi Bouzid",    "region": "Centre-Ouest"},
    {"name": "Cebbala Ouled Asker","lat": 35.2333, "lon":  9.5333, "governorate": "Sidi Bouzid",    "region": "Centre-Ouest"},
    {"name": "Souk Jedid",         "lat": 35.2500, "lon":  9.4833, "governorate": "Sidi Bouzid",    "region": "Centre-Ouest"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE GABES  (Sud-Est)
    # ═══════════════════════════════════════════════════════════
    {"name": "Gabes",              "lat": 33.8815, "lon": 10.0982, "governorate": "Gabes",          "region": "Sud-Est"},
    {"name": "El Hamma",           "lat": 33.8833, "lon":  9.7833, "governorate": "Gabes",          "region": "Sud-Est"},
    {"name": "Mareth",             "lat": 33.6333, "lon": 10.3000, "governorate": "Gabes",          "region": "Sud-Est"},
    {"name": "Matmata",            "lat": 33.5333, "lon":  9.9667, "governorate": "Gabes",          "region": "Sud-Est"},
    {"name": "Metouia",            "lat": 34.0333, "lon": 10.0000, "governorate": "Gabes",          "region": "Sud-Est"},
    {"name": "Ghannouch",          "lat": 33.9333, "lon": 10.0333, "governorate": "Gabes",          "region": "Sud-Est"},
    {"name": "Chenini Nahal",      "lat": 33.9000, "lon": 10.1333, "governorate": "Gabes",          "region": "Sud-Est"},
    {"name": "Nouvelle Matmata",   "lat": 33.6667, "lon": 10.0333, "governorate": "Gabes",          "region": "Sud-Est"},
    {"name": "Oudhref",            "lat": 33.8667, "lon": 10.0500, "governorate": "Gabes",          "region": "Sud-Est"},
    {"name": "Menzel El Habib",    "lat": 33.8333, "lon":  9.9833, "governorate": "Gabes",          "region": "Sud-Est"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE MEDENINE  (Sud-Est)
    # ═══════════════════════════════════════════════════════════
    {"name": "Medenine",           "lat": 33.3549, "lon": 10.5055, "governorate": "Medenine",       "region": "Sud-Est"},
    {"name": "Djerba Houmt Souk",  "lat": 33.8750, "lon": 10.8564, "governorate": "Medenine",       "region": "Sud-Est"},
    {"name": "Djerba Midoun",      "lat": 33.8081, "lon": 10.9947, "governorate": "Medenine",       "region": "Sud-Est"},
    {"name": "Djerba Ajim",        "lat": 33.7333, "lon": 10.7500, "governorate": "Medenine",       "region": "Sud-Est"},
    {"name": "Zarzis",             "lat": 33.5036, "lon": 11.1122, "governorate": "Medenine",       "region": "Sud-Est"},
    {"name": "Ben Guerdane",       "lat": 33.1333, "lon": 11.2167, "governorate": "Medenine",       "region": "Sud-Est"},
    {"name": "Beni Khedache",      "lat": 33.2500, "lon": 10.1833, "governorate": "Medenine",       "region": "Sud-Est"},
    {"name": "Sidi Makhlouf",      "lat": 33.4667, "lon": 10.4167, "governorate": "Medenine",       "region": "Sud-Est"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE TATAOUINE  (Sud-Est)
    # ═══════════════════════════════════════════════════════════
    {"name": "Tataouine",          "lat": 32.9292, "lon": 10.4518, "governorate": "Tataouine",      "region": "Sud-Est"},
    {"name": "Ghomrassen",         "lat": 33.0667, "lon": 10.3333, "governorate": "Tataouine",      "region": "Sud-Est"},
    {"name": "Remada",             "lat": 32.3167, "lon": 10.3833, "governorate": "Tataouine",      "region": "Sud-Est"},
    {"name": "Dehiba",             "lat": 32.0000, "lon": 10.7000, "governorate": "Tataouine",      "region": "Sud-Est"},
    {"name": "Bir Lahmar",         "lat": 33.1833, "lon": 10.4667, "governorate": "Tataouine",      "region": "Sud-Est"},
    {"name": "Smar",               "lat": 33.0667, "lon": 10.5167, "governorate": "Tataouine",      "region": "Sud-Est"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE GAFSA  (Sud-Ouest)
    # ═══════════════════════════════════════════════════════════
    {"name": "Gafsa",              "lat": 34.4250, "lon":  8.7842, "governorate": "Gafsa",          "region": "Sud-Ouest"},
    {"name": "Metlaoui",           "lat": 34.3167, "lon":  8.4000, "governorate": "Gafsa",          "region": "Sud-Ouest"},
    {"name": "Redeyef",            "lat": 34.3667, "lon":  8.1500, "governorate": "Gafsa",          "region": "Sud-Ouest"},
    {"name": "Moularès",           "lat": 34.2833, "lon":  8.2667, "governorate": "Gafsa",          "region": "Sud-Ouest"},
    {"name": "El Guettar",         "lat": 34.3333, "lon":  8.9500, "governorate": "Gafsa",          "region": "Sud-Ouest"},
    {"name": "Sened",              "lat": 34.3833, "lon":  9.1333, "governorate": "Gafsa",          "region": "Sud-Ouest"},
    {"name": "Belkhir",            "lat": 34.4000, "lon":  9.0333, "governorate": "Gafsa",          "region": "Sud-Ouest"},
    {"name": "Sidi Aich",          "lat": 34.6500, "lon":  8.6167, "governorate": "Gafsa",          "region": "Sud-Ouest"},
    {"name": "El Ksar (Gafsa)",    "lat": 34.4333, "lon":  8.8167, "governorate": "Gafsa",          "region": "Sud-Ouest"},
    {"name": "Zannouch",           "lat": 34.3833, "lon":  8.5333, "governorate": "Gafsa",          "region": "Sud-Ouest"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE TOZEUR  (Sud-Ouest)
    # ═══════════════════════════════════════════════════════════
    {"name": "Tozeur",             "lat": 33.9197, "lon":  8.1336, "governorate": "Tozeur",         "region": "Sud-Ouest"},
    {"name": "Nefta",              "lat": 33.8833, "lon":  7.8833, "governorate": "Tozeur",         "region": "Sud-Ouest"},
    {"name": "Degache",            "lat": 33.9667, "lon":  8.2333, "governorate": "Tozeur",         "region": "Sud-Ouest"},
    {"name": "Tamerza",            "lat": 34.3833, "lon":  7.9500, "governorate": "Tozeur",         "region": "Sud-Ouest"},
    {"name": "Hazoua",             "lat": 33.8000, "lon":  7.6167, "governorate": "Tozeur",         "region": "Sud-Ouest"},

    # ═══════════════════════════════════════════════════════════
    #  GOUVERNORAT DE KEBILI  (Sud-Ouest)
    # ═══════════════════════════════════════════════════════════
    {"name": "Kebili",             "lat": 33.7047, "lon":  8.9711, "governorate": "Kebili",         "region": "Sud-Ouest"},
    {"name": "Douz",               "lat": 33.4667, "lon":  9.0167, "governorate": "Kebili",         "region": "Sud-Ouest"},
    {"name": "Souk Lahad",         "lat": 33.6667, "lon":  8.9167, "governorate": "Kebili",         "region": "Sud-Ouest"},
    {"name": "El Golaa",           "lat": 33.4833, "lon":  8.4333, "governorate": "Kebili",         "region": "Sud-Ouest"},
    {"name": "Jemna",              "lat": 33.5833, "lon":  8.8333, "governorate": "Kebili",         "region": "Sud-Ouest"},
    {"name": "Faouar",             "lat": 33.3833, "lon":  8.5000, "governorate": "Kebili",         "region": "Sud-Ouest"},
]


# ──────────────────────────────────────────────────────────────
#  HELPER FUNCTIONS
# ──────────────────────────────────────────────────────────────

def get_all_cities():
    """Return the full list of Tunisian cities."""
    return TUNISIA_CITIES


def get_cities_by_region(region: str):
    """Filter cities by region (Nord, Nord-Est, Nord-Ouest, Centre,
    Centre-Ouest, Sud-Est, Sud-Ouest)."""
    return [c for c in TUNISIA_CITIES if c["region"] == region]


def get_cities_by_governorate(governorate: str):
    """Filter cities by governorate name (e.g. 'Sfax', 'Nabeul')."""
    return [c for c in TUNISIA_CITIES if c["governorate"] == governorate]


def get_governorate_capitals():
    """Return only the 24 governorate capitals (first entry per governorate)."""
    seen = set()
    capitals = []
    for city in TUNISIA_CITIES:
        gov = city["governorate"]
        if gov not in seen:
            seen.add(gov)
            capitals.append(city)
    return capitals


def get_city_by_name(name: str):
    """Return a single city dict by exact name match, or None."""
    for c in TUNISIA_CITIES:
        if c["name"] == name:
            return c
    return None


def list_governorates():
    """Return a sorted list of all 24 governorate names."""
    return sorted({c["governorate"] for c in TUNISIA_CITIES})


def list_regions():
    """Return a sorted list of all region names."""
    return sorted({c["region"] for c in TUNISIA_CITIES})


def city_count():
    """Return the total number of cities in the registry."""
    return len(TUNISIA_CITIES)


# ──────────────────────────────────────────────────────────────
#  CLI quick-check
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"Total cities registered: {city_count()}")
    print(f"Governorates ({len(list_governorates())}): {', '.join(list_governorates())}")
    print(f"Regions: {', '.join(list_regions())}")
    for region in sorted(list_regions()):
        cities = get_cities_by_region(region)
        print(f"\n  [{region}] — {len(cities)} cities")
        for c in cities:
            print(f"    • {c['name']:25s} ({c['governorate']:15s})  "
                  f"lat={c['lat']:.4f}  lon={c['lon']:.4f}")

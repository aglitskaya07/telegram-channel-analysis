"""
Конфигурация брендов для анализа.
Скопируйте этот файл как brands_config.py и заполните своими данными.
"""

# Основной бренд (за кого анализируем)
PRIMARY_BRAND_NAME = "My Brand"
PRIMARY_BRAND_NAME_GENITIVE = "My Brand"
PRIMARY_BRAND_URL_PATTERN = r'mybrand'

# Паттерны для поиска основного бренда в тексте постов (regex)
PRIMARY_BRAND_PATTERNS = {
    "My Brand (общее)": r'\bmy\s*brand[a-z]*\b',
    "MyBrand Product": r'\bmybrand\s*product[a-z]*\b',
}

# Конкуренты и их паттерны
COMPETITOR_PATTERNS = {
    "Competitor A": {
        "Competitor A": r'\bcompetitor[\s_-]*a[a-z]*\b',
    },
    "Competitor B": {
        "Competitor B": r'\bcompetitor[\s_-]*b[a-z]*\b',
    },
}

# Если один из конкурентов используется только как видеохостинг
# (например, посты просто дублируются на эту платформу) —
# укажите его имя и паттерны. Иначе поставьте None.
HOSTING_BRAND = None
HOSTING_VIDEO_PATTERN = None
HOSTING_EDITORIAL_PATTERN = None
HOSTING_ALSO_ON_PATTERN = None

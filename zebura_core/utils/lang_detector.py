from langdetect import detect

language_map = {
    'en': 'English',
    'fr': 'French',
    'de': 'German',
    'es': 'Spanish',
    'it': 'Italian',
    'pt': 'Portuguese',
    'nl': 'Dutch',
    'ru': 'Russian',
    'HU': 'Hungarian',
    'SV': 'Swedish',
    'ja': 'Japanese',
    'zh': 'Chinese',
    'zh-cn': 'Chinese'
    # 添加更多语言映射
}
def langcode2name(langcode):
    if langcode in language_map:
        return language_map[langcode]
    else:
        return langcode

def langname2code(langname):
    for code, name in language_map.items():
        if name.lower() == langname.lower():
            return code
    return None

def detect_language(text: str) -> str:
    try:
        lang = detect(text)
        if lang in language_map:
            return language_map[lang]
        else:
            return lang
    except:
        return None


# Example usage
if __name__ == '__main__':
    lang =detect_language("abie 1-851-8n, i@@@@") # 'en'
    print(lang)
    print(langname2code(lang))
    lang =detect_language("これはテストです") # 'ja'
    print(lang)
    print(langname2code(lang))
    lang =detect_language("这是一个测试test") # 'zh-cn'
    print(lang)
    print(langname2code(lang))
    lang =detect_language("decFondeoInicial, decImporteAbonoClte, decImporteCancel, decImporteDevol, decImporteDevolTC, decImporteImpuestos") # 'zh-cn'
    print(lang)
    code = langname2code(lang)
    print(langcode2name(code))
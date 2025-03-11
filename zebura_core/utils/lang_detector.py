from langdetect import detect, detect_langs

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
    'zh-cn': 'Chinese',
    'da': 'Danish',
    'fi': 'Finnish',
    'cy': 'Welsh',
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
        result = detect_langs(text)   # probabilities
        langCode = result[0].lang
        prob = result[0].prob
        if prob > 0.9:
            return langCode
        return None
    except:
        return None


# Example usage
if __name__ == '__main__':
    sents = ['This is a pen', 'これはペンです', '这是一个苹果', 'これはテストです', '这是一个测试test', 'decFondeoInicial, decImporteAbonoClte, decImporteCancel, decImporteDevol, decImporteDevolTC, decImporteImpuestos']
    for sent in sents:
        lang = detect_language(sent)
        if lang is not None:
            langName = langcode2name(lang)
            print(f"{sent} : {langName}")
            print(f"{langName} : {langname2code(langName)}")
        else:
            print(f"{sent} : Language detection failed")

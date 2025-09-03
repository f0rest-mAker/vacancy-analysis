import re


def normalize_text(text, word_tokenize, stop_words, morph):
    tokens = word_tokenize(text)
    tokens = [t.lower() for t in tokens if t.lower() not in stop_words and t.strip() != '']
    candidate_tokens = [
        morph.parse(token)[0].normal_form
        for token in tokens
        if re.match(r'^[а-яa-z0-9\-_]+$', token, re.IGNORECASE) and not(token.isdigit())
    ]
    return candidate_tokens


def replace_multiword_skills(text, multiword_skills):
    for skill in multiword_skills:
        words = skill.split("_")
        pattern = re.compile(r'\b' + r'\s+'.join(words) + r'\b', flags=re.IGNORECASE)
        text = pattern.sub(skill, text)
    return text
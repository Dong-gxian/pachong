import re
def delete_tags(html):
    pattern = re.compile('<.*?>', re.S)
    return re.sub(pattern,'',html)

def delete_spaces(strs):
    return "".join(strs).strip()
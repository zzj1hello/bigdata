import jieba 

def extract_kw(sent):
    res = []
    for kw in jieba.lcut(sent):
        if kw in ['谷', '帮', '客']:
            continue
        if kw =='传智播': kw = "传智播客"
        if kw == '院校': kw = '院校帮'
        if kw == '博学': kw = '博学谷'
        res.append((kw, 1))
    return res

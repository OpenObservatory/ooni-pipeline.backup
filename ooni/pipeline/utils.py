def convert2unicode(dictionary):
    for k, v in dictionary.iteritems():
        if isinstance(v, str):
            dictionary[k] = unicode(v, errors='replace')
        elif isinstance(v, dict):
            convert2unicode(v)

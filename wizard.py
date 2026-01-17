def _timestamp_to_epoch_and_str(val):
    if val is None:
        return (None, None)

    s = str(val)

    # normalize whitespace (NBSP, tabs)
    s = s.replace(u"\u00A0", " ").replace("\t", " ").strip()
    while "  " in s:
        s = s.replace("  ", " ")

    if not s:
        return (None, None)

    # unify comma separation: "YYYY/MM/DD, HH:MM" -> "YYYY/MM/DD HH:MM"
    s = s.replace(", ", " ").replace(",", " ")
    while "  " in s:
        s = s.replace("  ", " ")

    # drop trailing 'Z' or timezone words, and milliseconds
    # examples: "12:30Z" -> "12:30", "12:30:00.123" -> "12:30:00"
    s = re.sub(r"Z$", "", s).strip()
    s = re.sub(r"(\d{2}:\d{2}:\d{2})\.\d+", r"\1", s).strip()
    s = re.sub(r"\s+(UTC|GMT)\b.*$", "", s).strip()

    # epoch sec/ms
    if re.fullmatch(r"\d+", s):
        try:
            num = int(s)
            if num > 1000000000000:
                num //= 1000
            dt = datetime.datetime.utcfromtimestamp(num)
            return (int(num), dt.strftime("%Y-%m-%d %H:%M:%S"))
        except Exception:
            return (None, s)

    fmts = [
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y/%m/%d",
        "%Y-%m-%d",
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y",
    ]

    for fmt in fmts:
        try:
            dt = datetime.datetime.strptime(s, fmt)
            epoch = int((dt - datetime.datetime(1970, 1, 1)).total_seconds())
            return (epoch, dt.strftime("%Y-%m-%d %H:%M:%S"))
        except Exception:
            continue

    # Parsing failed -> keep original string for comment/debug
    return (None, s)

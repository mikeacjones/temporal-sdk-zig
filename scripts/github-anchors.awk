# Prints the anchors GitHub gives a Markdown file's headings, one per line as
# `<anchor><TAB><ok|stub>`, for scripts/parity-check.sh. A heading is a stub when the first
# line under it is a `<!-- stub: ... -->` comment.
#
# It reads only a small, plain subset of Markdown, and refuses everything else that could
# create or hide a heading, so it never reports an anchor GitHub doesn't make:
#
# - A heading is a top-level ATX heading: 1 to 6 `#` at column 0 and a space, outside a
#   fenced code block (opened and closed by ``` or ~~~ at column 0) and outside an HTML
#   comment (`<!--` at column 0, up to the line holding `-->`).
# - Its text may hold plain characters, single-backtick code spans, inline links and images,
#   which become their text, `_` inside a word, and single `_` emphasis around words.
#   The anchor is that text slugged as github-slugger does: lowercased, punctuation and
#   symbols dropped, each space a hyphen, and a slug already taken given the first free `-1`,
#   `-2`, ... suffix.
#
# Each refused line is printed as `!<TAB><line><TAB><reason>`, and the caller fails: a fence
# or `<!--` not at column 0, two comment openers on one line, an unclosed fence or comment,
# an HTML block or HTML heading, a heading-like line that is indented or follows a list or
# quote marker, a setext underline anywhere (top level, block quote, or list item), a carriage
# return inside a line, and heading text with other markup or characters this doesn't slug.
# A byte order mark before the first line is dropped, as GitHub drops it.
#
# Usage: LC_ALL=C awk -f scripts/github-anchors.awk README.md

BEGIN { for (i = 1; i < 256; i++) ord[sprintf("%c", i)] = i }

function refuse(n, reason) { printf "!\t%d\t%s\n", n, reason }

function utf8(cp) {
  if (cp < 128) return sprintf("%c", cp)
  if (cp < 2048) return sprintf("%c%c", 192 + int(cp / 64), 128 + cp % 64)
  return sprintf("%c%c%c", 224 + int(cp / 4096), 128 + int(cp / 64) % 64, 128 + cp % 64)
}

# Single-backtick code spans become their text. Outside them, characters that start markup
# this doesn't read (escapes, entities, HTML, autolinks) are refused, and so is any `_` but
# the two kinds whose rendering is certain: one inside a word, which stays, and single `_`
# delimiters that open before a word and close after one, strictly alternating, which are
# emphasis and are dropped.
function code_spans(t,   o, i, j, c, first) {
  o = ""; open_em = 0; first = 1
  while ((i = index(t, "`")) > 0) {
    o = o underscores(substr(t, 1, i - 1), first, 0); if (why != "") return ""
    first = 0; t = substr(t, i + 1)
    j = index(t, "`"); c = substr(t, 1, j - 1)
    if (j <= 1 || substr(t, j + 1, 1) == "`" || c ~ /[][]|^ | $/) { why = "a code span this check doesn't read"; return "" }
    o = o c; t = substr(t, j + 1)
  }
  o = o underscores(t, first, 1)
  if (why == "" && open_em) why = "an emphasis underscore with no closing one"
  return why == "" ? o : ""
}

# A stretch of heading text outside code spans, with its emphasis underscores removed.
# `first` and `last` say whether it starts and ends the heading rather than touching a code
# span; an underscore next to a code span is refused.
function underscores(t, first, last,   o, i, n, c, p, q, pw, qw) {
  if (t ~ /[\\&<>]/) { why = "markup this check doesn't read (\\ & < > outside a code span)"; return "" }
  o = ""; n = length(t)
  for (i = 1; i <= n; i++) {
    c = substr(t, i, 1)
    if (c != "_") { o = o c; continue }
    p = substr(t, i - 1, 1); q = substr(t, i + 1, 1)
    pw = p ~ /[A-Za-z0-9]/; qw = q ~ /[A-Za-z0-9]/
    if (pw && qw) o = o c
    else if (!open_em && qw && (i == 1 ? first : p == " ")) open_em = 1
    else if (open_em && pw && (i == n ? last : q == " ")) open_em = 0
    else { why = "an underscore this check can't be sure of"; return "" }
  }
  return o
}

# Inline links and images, innermost first, become their text: `[text](dest)` or
# `[text](dest "title")` with no spaces or brackets in dest. Any other link syntax is refused.
function links(o,   pre, text, rest) {
  while (match(o, /!?\[[^][]*\]\(/)) {
    pre = substr(o, 1, RSTART - 1)
    text = substr(o, RSTART, RLENGTH - 2); sub(/^!?\[/, "", text)
    rest = substr(o, RSTART + RLENGTH)
    if (!match(rest, /^[^ ()<>"]*( "[^"]*")?\)/)) { why = "a link this check doesn't read"; return "" }
    o = pre text substr(rest, RLENGTH + 1)
  }
  if (o ~ /[][]/) { why = "brackets that aren't an inline link"; return "" }
  return o
}

# github-slugger, for ASCII and the Latin-1, Latin Extended-A, punctuation, arrow and math
# characters whose Unicode category this knows; any other character is refused.
function slugify(t,   s, i, n, b, c, cp, extra, k) {
  s = ""; n = length(t); i = 1
  while (i <= n) {
    c = substr(t, i, 1); b = ord[c]
    if (b < 128) {
      i++
      if (c ~ /[A-Z]/) s = s tolower(c)
      else if (c ~ /[a-z0-9_-]/) s = s c
      else if (c == " ") s = s "-"
      else if (b < 32 || b == 127) { why = "a control character or tab"; return "" }
      continue
    }
    if (b >= 224 && b < 240) { extra = 2; cp = b - 224 } else if (b >= 192 && b < 224) { extra = 1; cp = b - 192 } else { why = "a character outside the Basic Multilingual Plane, or invalid UTF-8"; return "" }
    for (k = 1; k <= extra; k++) cp = cp * 64 + ord[substr(t, i + k, 1)] - 128
    i += extra + 1
    if (cp >= 192 && cp <= 222 && cp != 215) s = s utf8(cp + 32)
    else if (cp >= 223 && cp <= 255 && cp != 247) s = s utf8(cp)
    else if (cp == 215 || cp == 247) continue
    else if (cp == 170 || cp == 178 || cp == 179 || cp == 181 || cp == 185 || cp == 186 || cp == 188 || cp == 189 || cp == 190) s = s utf8(cp)
    else if (cp >= 161 && cp <= 191 && cp != 173) continue
    else if (cp >= 256 && cp <= 383 && cp != 304) {
      if ((cp <= 311 || (cp >= 330 && cp <= 375)) && cp % 2 == 0) s = s utf8(cp + 1)
      else if (((cp >= 313 && cp <= 328) || (cp >= 377 && cp <= 382)) && cp % 2 == 1) s = s utf8(cp + 1)
      else if (cp == 376) s = s utf8(255)
      else s = s utf8(cp)
    }
    else if ((cp >= 8208 && cp <= 8231) || (cp >= 8240 && cp <= 8286) || (cp >= 8592 && cp <= 8959)) continue
    else { why = sprintf("the character U+%04X", cp); return "" }
  }
  return s
}

function heading(text, n,   slug, original) {
  why = ""
  sub(/^[ \t]+/, "", text); sub(/[ \t]+$/, "", text)
  text = code_spans(text)
  if (why == "") text = links(text)
  if (why == "") slug = slugify(text)
  if (why == "" && slug == "") why = "an empty anchor"
  if (why != "") { refuse(n, why); return }
  original = slug
  while (slug in seen) { seen[original]++; slug = original "-" seen[original] }
  seen[slug] = 0
  pending = ++count; anchor[count] = slug; state[count] = "ok"
}

{
  line = $0
  if (NR == 1) sub(/^\357\273\277/, "", line)
  sub(/\r$/, "", line)
  if (index(line, "\r")) { refuse(NR, "a carriage return inside a line, which is a line break on GitHub"); text_before = 0; next }
  blank = line ~ /^[ \t]*$/

  if (pending && !blank) { if (line ~ /^<!--[ \t]*stub:/) state[pending] = "stub"; pending = 0 }

  if (fence != "") {
    if (line ~ /^[ \t]+(```|~~~)/) refuse(NR, "a fence line that isn't at column 0")
    else if (substr(line, 1, 1) == substr(fence, 1, 1)) {
      m = line; sub(/[ \t]+$/, "", m)
      if ((m !~ /[^`]/ || m !~ /[^~]/) && length(m) >= length(fence)) fence = ""
    }
    text_before = 0; next
  }
  if (comment) {
    if (index(line, "-->")) { comment = 0; if (index(substr(line, index(line, "-->")), "<!--")) refuse(NR, "a second <!-- on a line") }
    text_before = 0; next
  }
  if (html) { if (blank) html = 0; next }
  if (blank) { text_before = 0; next }

  rest = line; openers = gsub(/<!--/, "", rest)
  if (openers > 1) { refuse(NR, "more than one <!-- on a line"); next }
  if (openers == 1) {
    if (substr(line, 1, 4) != "<!--") { refuse(NR, "a <!-- that isn't at column 0"); next }
    if (substr(line, 5) ~ /^-?>/) { refuse(NR, "a <!--> or <!---> comment"); next }
    if (!index(substr(line, 5), "-->")) comment = NR
    text_before = 0; next
  }

  inner = line; marked = 0
  while (sub(/^[ \t]*>/, "", inner) || sub(/^[ \t]*([-+*]|[0-9]+[.)])([ \t]|$)/, "", inner)) marked = 1
  if (marked && inner ~ /^[ \t]*(#|```|~~~|<)/) { refuse(NR, "a heading, fence or HTML after a list or quote marker"); next }
  if (line ~ /^[ \t]*</) { refuse(NR, "an HTML block"); html = 1; next }

  if (line ~ /^(```|~~~)/) {
    fence = line; sub(/[^`~].*$/, "", fence)
    if (substr(fence, 1, 1) == "`") sub(/~.*$/, "", fence); else sub(/`.*$/, "", fence)
    if (length(fence) < 3 || (substr(fence, 1, 1) == "`" && index(substr(line, length(fence) + 1), "`"))) { refuse(NR, "a fence-like line this check doesn't read"); fence = "" }
    else fence_line = NR
    next
  }
  if (line ~ /^[ \t]+(```|~~~|#)/) { refuse(NR, "an indented fence or heading-like line"); next }
  # A line of = or - under text is a setext heading, at the top level or in a block quote or
  # list item; GitHub anchors it wherever it is, so it is refused wherever it is.
  setext = line; while (sub(/^[ \t]*>/, "", setext)) {}
  if (text_before && setext ~ /^[ \t]*(=+|-+)[ \t]*$/) { refuse(NR, "a setext heading underline"); text_before = 0; next }
  if (tolower(line) ~ /<h[1-6]([ \t>\/]|$)/) { refuse(NR, "an HTML heading"); next }
  if (NR == 1 && line ~ /^---/) { refuse(NR, "front matter"); next }
  if (line ~ /^\$\$/) { refuse(NR, "a math block"); next }

  if (match(line, /^#+([ \t]|$)/) && RLENGTH - (substr(line, RLENGTH, 1) ~ /[ \t]/) <= 6) {
    text = substr(line, RLENGTH + 1)
    sub(/[ \t]+$/, "", text)
    if (text ~ /^#+$/) text = ""; else sub(/[ \t]+#+$/, "", text)
    heading(text, NR)
    text_before = 0; next
  }
  text_before = 1
}

END {
  if (fence != "") refuse(fence_line, "an unclosed fence")
  if (comment) refuse(comment, "an unclosed <!-- comment")
  for (i = 1; i <= count; i++) printf "%s\t%s\n", anchor[i], state[i]
}

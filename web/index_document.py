"""Pure selection of the secure or insecure index document."""

INSECURE_FOOTER_START = b"<!-- CRATEDIGGER_INSECURE_FOOTER_START -->"
INSECURE_FOOTER_END = b"<!-- CRATEDIGGER_INSECURE_FOOTER_END -->"


def render_index_document(template: bytes, *, insecure: bool) -> bytes:
    """Select the one static insecure footer block from the index template."""
    if (
        template.count(INSECURE_FOOTER_START) != 1
        or template.count(INSECURE_FOOTER_END) != 1
    ):
        raise RuntimeError(
            "index template must contain exactly one insecure footer block"
        )
    footer_start = template.index(INSECURE_FOOTER_START)
    footer_end = template.index(INSECURE_FOOTER_END)
    if footer_end < footer_start:
        raise RuntimeError(
            "index template must contain exactly one insecure footer block"
        )
    block_end = footer_end + len(INSECURE_FOOTER_END)
    if insecure:
        return template
    return template[:footer_start] + template[block_end:]

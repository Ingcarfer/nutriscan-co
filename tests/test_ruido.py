from nutriscan.ruido import degradar

TEXTO = "harina de trigo, azucar, aceite de palma, jarabe de glucosa, sal, emulsificante lecitina de soya"


def test_nivel_cero_no_cambia_nada():
    assert degradar(TEXTO, 0) == TEXTO
    assert degradar(None, 0.5) is None


def test_es_reproducible():
    assert degradar(TEXTO, 0.3) == degradar(TEXTO, 0.3)
    assert degradar(TEXTO, 0.3, semilla=1) != degradar(TEXTO, 0.3, semilla=2)


def test_mas_ruido_cambia_mas_palabras():
    originales = TEXTO.split()

    def intactas(nivel):
        return sum(p in originales for p in degradar(TEXTO * 20, nivel).split())

    assert intactas(0.1) > intactas(0.3) > intactas(0.6)


def test_truncado_conserva_el_inicio():
    r = degradar(TEXTO, 0.5, tipo="truncado")
    assert TEXTO.startswith(r)
    assert len(r.split()) == round(len(TEXTO.split()) * 0.5)

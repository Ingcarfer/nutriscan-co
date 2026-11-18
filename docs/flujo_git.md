# Cómo trabajamos con Git (guía rápida)

1. **`main`** es la rama principal y siempre debe funcionar.
2. Cada bloque se hace en su propia rama, por ejemplo `bloque-1-ingesta`.
3. Al terminar un bloque:
   - En GitHub Desktop: **Publish branch**.
   - Luego **Create Pull Request** y revisar que la rama base sea `main`.
   - El compañero revisa el PR y lo aprueba (merge).
4. Antes de empezar un bloque nuevo: en GitHub Desktop, cambiar a `main` y hacer **Fetch/Pull**
   para traer los cambios del compañero.

Los datos (`data/`) **nunca** se suben a GitHub: pesan varios GB y se regeneran con los scripts.

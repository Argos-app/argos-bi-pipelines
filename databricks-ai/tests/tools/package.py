"""Empacota somente entregáveis, sem dependências de teste ou credenciais."""
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

root=Path(__file__).resolve().parents[2]
destination=root/'docs/entregaveis/pacotes'
destination.mkdir(exist_ok=True)
with ZipFile(destination/'argos-ai-bi-importacao.zip','w',ZIP_DEFLATED) as archive:
    for path in sorted((root/'docs/entregaveis/copiar_colar').rglob('*.ipynb')):
        archive.write(path,'notebooks/'+path.relative_to(root/'docs/entregaveis/copiar_colar').as_posix())
    archive.write(root/'docs/CONFIGURACAO_DATABRICKS.md','README.md')
    archive.write(root/'docs/job-settings.json','job-settings.json')
    archive.write(root/'docs/VERIFICACAO.md','VERIFICACAO.md')
    
    for sql in sorted((root/'dashboards').glob('*.sql')):
        archive.write(sql,'dashboards/'+sql.name)
with ZipFile(destination/'argos-ai-bi-completo.zip','w',ZIP_DEFLATED) as archive:
    for path in sorted(root.rglob('*')):
        if path.is_file() and '__pycache__' not in path.parts and 'pacotes' not in path.relative_to(root).parts and path.suffix!='.pyc':
            archive.write(path,'databricks-ai/'+path.relative_to(root).as_posix())
print('Pacotes gerados:')
for name in ('argos-ai-bi-importacao.zip','argos-ai-bi-completo.zip'):
    p=destination/name
    with ZipFile(p) as archive:
        assert archive.testzip() is None
        print(name,len(archive.namelist()),'arquivos;',p.stat().st_size,'bytes')

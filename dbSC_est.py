from sqlalchemy import create_engine, select, text, insert
import pandas as pd, datetime

def mk_sc_est():

	engine = create_engine('oracle://cscliente:X4ap968qk7#yXT6HkYvn@168.194.255.144:1521/cs0084p')
	con = engine.connect()

	# Definir a data
	data = datetime.date.today()
	fdata = data.strftime('%d/%m/%Y')

	planilha = "E:/Pasta servidores/sc.xlsx"
	dfreq = pd.read_excel(planilha, sheet_name="Suprimento")
	dfreq.fillna(0, inplace=True)

	for index, row in dfreq.iterrows():
		# Variaveis
		if row['REQUISIÇÃO'] == 'E':
			objc = row['COD SETOR']
			cod_material = row['COD']
			qtd = row['QTD SOL']
			just = f'SOLICITADO POR: {row['SOLICITANTE']} // APROVADO POR: HUGO OKAHARA // APLICAÇÃO: {row['DESCRIÇÃO SERVIÇOS']}'
			if row['OBSERVAÇÃO'] == 0:
				inf_forn = ''
			else:
				inf_forn = row['OBSERVAÇÃO']

			# Gera o numero de solicitação
			novo = con.execute(text("select max(nr_solicitacao)+1 from material.solicitacaocompra"))
			newsc = novo.all()[0][0]
			print(newsc)

			sql = f"""
			insert into material.solicitacaocompra(
			NR_SOLICITACAO,
			COD_GRUPOEMPRESA,
			COD_GRUPOEMPRESA_DESTINO,
			COD_EMPRESA_DESTINO,
			COD_FILIAL_DESTINO,
			DATAUTILIZACAOPREVISTA,
			DATA,
			COD_FUNCIONARIO,
			COD_MATERIAL,
			QTDESOLICITADA,
			COD_ALMOXARIFADO,
			COD_OBJETOCUSTO,
			OBSERVACAO,
			INFORMACAO_FORNECEDOR,
			ORIGEM
			)

			values(
			{newsc},
			1,
			1,
			1,
			1,
			to_date('{fdata}', 'dd/mm/yyyy'),
			to_date('{fdata}', 'dd/mm/yyyy'),
			3331,
			{cod_material},
			{qtd},
			8,
			{objc},
			'{just}',
			'{inf_forn}',
			'I'
			)
			"""

			con.execute(text(sql))
			con.commit()

			dfreq.iloc[index, 6] = ''
			dfreq.iloc[index, 7] = newsc
			dfreq.to_excel(planilha, sheet_name='Suprimento', index=False)
		else:
			continue

	con.close()

if __name__ == '__main__':
	mk_sc_est()

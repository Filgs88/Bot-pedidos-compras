from sqlalchemy import create_engine, text
import datetime, pandas as pd

def mk_sc_req():

	engine = create_engine('oracle://cscliente:X4ap968qk7#yXT6HkYvn@168.194.255.144:1521/cs0084p')
	con = engine.connect()

	# Definir a data
	data = datetime.date.today()
	fdata = data.strftime('%d/%m/%Y')


	# Definir numero de requisição
	novo = con.execute(text("select max(nrrequisicao)+1 from material.requisicaomaterial"))
	newreq = novo.all()[0][0]
	print(newreq)

	sql1 = f"""
	insert into material.requisicaomaterial(
	nrrequisicao,
	cod_empresa_req, 
	cod_filial_req, 
	cod_grupoempresa_req,
	cod_grupoempresa,
	cod_funcionario,  
	datarequisicao,
	numero_ordem_servico,
	ano_ordem_servico
	)

	values(
	{newreq},
	1, 
	1, 
	1, 
	1,
	3331,
	to_date('{fdata}', 'dd/mm/yyyy'),
	0,
	0
	)
	"""
	con.execute(text(sql1))
	con.commit()

	planilha = "E:/Pasta servidores/sc.xlsx"
	dfreq = pd.read_excel(planilha, sheet_name="Suprimento")
	dfreq.fillna(0, inplace=True)

	item = 0

	for index, row in dfreq.iterrows():
		item = item + 1
		if row['REQUISIÇÃO'] == 'R':
			objc = row['COD SETOR']
			codmaterial = row['COD']
			qtd = row['QTD SOL']
			just = f'SOLICITADO POR: {row['SOLICITANTE']} // APROVADO POR: HUGO OKAHARA // APLICAÇÃO: {row['DESCRIÇÃO SERVIÇOS']}'
			if row['OBSERVAÇÃO'] == 0:
				inf_forn = ''
			else:
				inf_forn = row['OBSERVAÇÃO']
			

			# Gera o numero de solicitação
			novo = con.execute(text("select max(nr_solicitacao)+1 from material.solicitacaocompra"))
			newsc = novo.all()[0][0]

			sql2 = f"""
			insert into material.itensrequisicaomaterial(
			NRREQUISICAO,
			ITEM,
			COD_MATERIAL,
			QUANTIDADE,
			COD_ALMOXARIFADO,
			COD_OBJETOCUSTO,
			GERASOLICITACAOCOMPRA,
			COD_GRUPOEMPRESA,
			COD_EMPRESA,
			COD_FILIAL,
			GERADOAUTOMATICAMENTE,
			RETIRADA_AUTOMATICA,
			PERMITE_RETIRADA_MANUAL,
			NR_SOLICITACAO,
			QTDESOLICITADA,
			OBSERVACAO,
			INFORMACAO_FORNECEDOR
			)

			values(
			{newreq},
			{item},
			{codmaterial},
			{qtd},
			8,
			{objc},
			'S',
			1,
			1,
			1,
			'N',
			'N',
			'S',
			{newsc},
			{qtd},
			'{just}',
			'{inf_forn}'
			)
			"""
			con.execute(text(sql2))
			con.commit()

			dfreq.iloc[index, 6] = newreq
			dfreq.iloc[index, 7] = newsc
			dfreq.to_excel(planilha, sheet_name='Suprimento', index=False)
		else:
			continue

	con.close()

if __name__ == '__main__':
	mk_sc_req()
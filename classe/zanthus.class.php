<?php

require_once("../class/pdvvenda.class.php");
require_once("../class/pdvitem.class.php");
require_once("../class/pdvfinalizador.class.php");
require_file("class/log.class.php");

class Zanthus{

	private $con;
	private $debug;
	private $pdvconfig;
	private $pdvvenda;
	private $pdvfinalizador;
	private $url_soap = "http://www.zanthusonline.com.br/manager_saas/saas_redirect.php5?wsdl";
	private $sessao_saas;
	private $soap;
	private $arr_movimento;
	public $temmovimento = false;

	function __construct(){
		$this->con = new Connection();

		$this->debug = $debug;
		$this->erros = array();

		if($this->debug){
			$file = fopen("../temp/vtex.log", "w+");
			fclose($file);
		}
	}

	private function setconfigwebservice(){
		if(strlen($this->pdvconfig->getestabelecimento()->getbeservidor()) > 0){
			$this->url_soap = $this->pdvconfig->getestabelecimento()->getbeservidor();
			$this->soap = new SoapClient($this->url_soap);
		}else{
			$this->soap = new SoapClient($this->url_soap);
			$str_xml = $this->soap->autenticarUsuario($this->pdvconfig->getestabelecimento()->getbeusuario(), $this->pdvconfig->getestabelecimento()->getbesenha(), $this->pdvconfig->getestabelecimento()->getbeempresa(), $this->pdvconfig->getestabelecimento()->getbelocal());
			$xml = simplexml_load_string($str_xml);
			$this->sessao_saas = (string) $xml->children()->status;
		}
	}

	function setpdvconfig($pdvconfig){
		$this->pdvconfig = $pdvconfig;
		$this->con = $this->pdvconfig->getconnection();
	}

	function getpdvvenda(){
		return $this->pdvvenda;
	}

	function getpdvfinalizador(){
		return $this->pdvfinalizador;
	}

	private function service_soap($xml, $tipo = "B"){
		$_SESSION["ERROR"] = "Tipo arquivo <b>$tipo</b>";
		$xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>".$xml;
		if(strlen($this->pdvconfig->getestabelecimento()->getbeservidor()) <= 0){
			$_param_header = new SoapVar(
					array(
				"service_sessao_saas" => $this->sessao_saas,
				"saas_service" => "integracao"
					)
					, SOAP_ENC_OBJECT);
			$header = new SoapHeader("AuthenticationInfo", "AuthenticationInfo", $_param_header, false);
			$this->soap->__setSoapHeaders($header);
		}

		try{
			$res = $this->soap->metodoIntegracaoGenerico("<![CDATA[$xml]]>");
			$res_xml = simplexml_load_string($res);

			$ok = (string) $res_xml->QUERY->CONTENT->OK;

			if($ok != "1" && !is_object($res_xml->QUERY->CONTENT->MOVIMENTOS)){
				$code = (string) $res_xml->CONTENT->ERRORS->ERROR->CODE;
				$description = (string) $res_xml->CONTENT->ERRORS->ERROR->DESCRIPTION;
				if(strlen($code) == 0){
					$code = (string) $res_xml->QUERY->CONTENT->ERRORS->ERROR->CODE;
					$description = (string) $res_xml->QUERY->CONTENT->ERRORS->ERROR->DESCRIPTION;
				}

				$_SESSION["ERROR"] .= "$code - $description";
				return false;
			}
		}catch(SoapFault $e){
			echo $e->faultstring;
			die;
		}

		return $res_xml;
	}

	public function cargaProduto(){
		setprogress(0, "Abrindo conexao");
		$this->setconfigwebservice();

		$this->enviar_departamento();
		return $this->enviar_produto();
	}

	public function cargaCliente(){
		$this->setconfigwebservice();

		return $this->enviar_cliente();
	}

	public function leituraVendas($dtulvenda = "", $hrultvenda = "00:00:00", $processardia = false){
		$this->temmovimento = false;
		// BUSCA AS TRIBUTACOES ICMS PDV
		$query = "SELECT DISTINCT tipoicms, infpdv ";
		$query .= "FROM icmspdv ";
		$query .= "WHERE codestabelec = {$this->pdvconfig->getestabelecimento()->getcodestabelec()} ";
		$res = $this->con->query($query);
		$arr_icmspdv = $res->fetchAll(2);

		$arr_tipoicms = array();
		foreach ($arr_icmspdv as $icmspdv) {
			$arr_tipoicms[$icmspdv["infpdv"]] = $icmspdv["tipoicms"];
		}

		$this->setconfigwebservice();

		$xml = new SimpleXMLElement("<xml></xml>");

		$ZMI = $xml->addChild("ZMI");
		$DATABASES = $ZMI->addChild("DATABASES");
		$DATABASE = $DATABASES->addChild("DATABASE");
		$DATABASE->addAttribute('AUTOCOMMIT_VALUE', '1000');
		$DATABASE->addAttribute('AUTOCOMMIT_ENABLED', '1');
		$DATABASE->addAttribute('HALTONERROR', '1');
		$DATABASE->addAttribute('NAME', 'MANAGER');
		$COMMANDS = $DATABASE->addChild("COMMANDS");
		$FUNCTION = $COMMANDS->addChild("FUNCTION");
		$BUSCAR_MOVIMENTOS = $FUNCTION->addChild("BUSCAR_MOVIMENTOS");
		$BUSCAR_MOVIMENTOS->addChild("COD_LOJA", $this->pdvconfig->getestabelecimento()->getcodestabelec(), null);
		if($processardia){
			$BUSCAR_MOVIMENTOS->addChild("DTA_INICIO", $dtulvenda."T00:00:00", null);
			$BUSCAR_MOVIMENTOS->addChild("DTA_FIM", $dtulvenda."T23:59:59", null);
		}else{
			$BUSCAR_MOVIMENTOS->addChild("DTA_INICIO", "2018-10-15T00:00:00", null);
			$BUSCAR_MOVIMENTOS->addChild("DTA_FIM", date("Y-m-d")."T23:59:59", null);
		}		
		$BUSCAR_MOVIMENTOS->addChild("M00ZZA01", "0", null);
		$BUSCAR_MOVIMENTOS->addChild("MARCAR", "0", null);

		$result = $this->service_soap($ZMI->asXML(), "L");

		if($result === FALSE){
			return FALSE;
		}

		$i = 0;
		foreach($result->QUERY->CONTENT->MOVIMENTOS->M00 AS $M00){
			$this->temmovimento = true;
			if(false){
				$log = new Log("zanthus-m00");
				
				$log->write((string) $M00->asXML());
			}


			$i++;
			$codestabelec = (string) $M00->M00ZA;
			$cupom = (string) $M00->M00AD;
			$numeroecf = (string) $M00->M00AC;						
			$data = (string) $M00->M00AF;
			$data = convert_date($data, "d-m-Y", "Y-m-d");
			$hora = (string) $M00->M00ZD;
			$numfuncao = (string) $M00->M00AE; //Número da função

			$this->arr_movimento[$i]["M00ZA"] = (string) $M00->M00ZA;
			$this->arr_movimento[$i]["M00ZB"] = (string) $M00->M00ZB;
			$this->arr_movimento[$i]["M00AF"] = $data;
			$this->arr_movimento[$i]["M00AC"] = (string) $M00->M00AC;
			$this->arr_movimento[$i]["M00AD"] = (string) $M00->M00AD;
			$this->arr_movimento[$i]["M00_CRO"] = (string) $M00->M00_CRO;
			$this->arr_movimento[$i]["M00_TRN"] = (string) $M00->M00_TRN;

			// 148 = Correção de pagamento
			// 158 = Sanguia
			// 154 = 
            if(in_array($numfuncao,array("158","148","154"))){
				continue;
			}

			if(strlen($hora) == 0){
				$hora = "00:00:00";
			}

			$status = "A";

			// Resumo do Cupom fiscal no PDV
			foreach($M00->ZAN_M01S->ZAN_M01 AS $M01){
				$codfunc = (string) $M01->M01AW;
				$cpfcnpj = (string) $M01->M01BV;				

				$cupomrelacionada = (string) $M01->M01BO;

				$res = $this->con->query("SELECT numfabricacao FROM ecf WHERE numeroecf = $numeroecf");
				$numfabricacao = $res->fetchColumn();
				$hora = (string) $M01->M01AU;
				$mm = substr($hora, -2);
				$hh = substr($hora, 0, -2);
				$hora = str_pad($hh, 2, "0", STR_PAD_LEFT).":".str_pad($mm, 2, "0", STR_PAD_LEFT).":00";

				if($cupomrelacionada != $cupom){
					$cupom = $cupomrelacionada;
					$status = "C";
				}
			}

			// Resumo da nota fiscal no PDV
			foreach($M00->ZAN_M45S->ZAN_M45 AS $M45){				
				
				$cpfcnpj = (string) $M45->M45BV;
				
				$operador = (string) $M45->M45AH;
				
				$seqecf = (string) $M45->M45XD;
				$chavecfe = (string) $M45->M45XB;
				$numfabricacao = (string) $M45->M45XE;
				$hora = (string) $M45->M45AU;
				$mm = substr($hora, -2);
				$hh = substr($hora, 0, -2);

				$hora = str_pad($hh, 2, "0", STR_PAD_LEFT).":".str_pad($mm, 2, "0", STR_PAD_LEFT).":00";
			}

			// Item vendido em cupom fiscal
			foreach($M00->ZAN_M03S AS $ZAN_M03){
				foreach($ZAN_M03 as $iten){
					$caixa = (string) $iten->M03ZB;
					$codproduto = (string) $iten->M03AH;
					$quantidade = (string) $iten->M03AO;
					$precounit = (string) $iten->M03AP / $quantidade;
					$desconto = (string) $iten->M03AQ;
					$acrescimo = (string) $iten->M03DJ;
					$totalitem = (string) $iten->M03AP;
					$tptribicms = (string) $iten->M03AJ;
					$tptribicms = $arr_tipoicms[$icmspdv[$tptribicms]];
					$aliqicms = (string) $iten->M03AY;
					$status_item = ((string) $iten->M03CF) == "1" ? "C" : "A";

					$pdvitem = new PdvItem();
					$pdvitem->setstatus($status_item);
					$pdvitem->setcodproduto($codproduto);
					$pdvitem->setquantidade($quantidade);
					$pdvitem->setpreco($precounit);
					$pdvitem->setdesconto($desconto);
					$pdvitem->setacrescimo($acrescimo);
					$pdvitem->settotal($totalitem);
					$pdvitem->settptribicms($arr_tipoicms[$tptribicms]);
					$pdvitem->setaliqicms($aliqicms);

					$found = FALSE;
					foreach(array_reverse($this->pdvvenda) as $pdvvenda){
						if($pdvvenda->getcupom() == $cupom && $pdvvenda->getcaixa() == $caixa && $pdvvenda->getstatus() == $status){
							$pdvvenda->pdvitem[] = $pdvitem;
							$found = TRUE;
							break;
						}
					}
					if(!$found){
						$pdvvenda = new PdvVenda();
						$pdvvenda->setcupom($cupom);
						$pdvvenda->setcaixa($caixa);
						$pdvvenda->setnumeroecf($numeroecf);
						$pdvvenda->setnumfabricacao($numfabricacao);
						$pdvvenda->setdata($data);
						$pdvvenda->sethora($hora);
						$pdvvenda->setcpfcnpj($cpfcnpj);
						$pdvvenda->setcodfunc($codfunc);
						$pdvvenda->setseqecf($cupom);
						$pdvvenda->setstatus($status);						
						$pdvvenda->setoperador($operador);						
						$pdvvenda->pdvitem[] = $pdvitem;
						$this->pdvvenda[] = $pdvvenda;
					}
				}
			}

			// Item de nota fiscal no PDV
			foreach($M00->ZAN_M43S AS $ZAN_M43){
				foreach($ZAN_M43 as $iten){
					$caixa = (string) $iten->M43ZB;
					$codproduto = (string) $iten->M43AH;
					$quantidade = (string) $iten->M43AO;
					$precounit = (string) $iten->M43AP / $quantidade;
					$desconto = (string) $iten->M43AQ;
					$acrescimo = (string) $iten->M43DJ;
					$totalitem = (string) $iten->M43AP;
					$tptribicms = (string) $iten->M43AJ;
					$tptribicms = $arr_tipoicms[$icmspdv[$tptribicms]];
					$aliqicms = (string) $iten->M43AY;
					$status_item = ((string) $iten->M43CF) == "1" ? "C" : "A";

					$pdvitem = new PdvItem();
					$pdvitem->setstatus($status_item);
					$pdvitem->setcodproduto($codproduto);
					$pdvitem->setquantidade($quantidade);
					$pdvitem->setpreco($precounit);
					$pdvitem->setdesconto($desconto);
					$pdvitem->setacrescimo($acrescimo);
					$pdvitem->settotal($totalitem);
					$pdvitem->settptribicms($tptribicms);
					$pdvitem->setaliqicms($aliqicms);

					$found = FALSE;
					foreach(array_reverse($this->pdvvenda) as $pdvvenda){
						if($pdvvenda->getcupom() == $cupom && $pdvvenda->getcaixa() == $caixa && $pdvvenda->getstatus() == $status){
							$pdvvenda->pdvitem[] = $pdvitem;
							$found = TRUE;
							break;
						}
					}
					if(!$found){
						$pdvvenda = new PdvVenda();
						$pdvvenda->setcupom($cupom);
						$pdvvenda->setcaixa($caixa);
						$pdvvenda->setnumeroecf($numeroecf);
						$pdvvenda->setnumfabricacao($numfabricacao);
						$pdvvenda->setdata($data);
						$pdvvenda->sethora($hora);
						$pdvvenda->setcpfcnpj($cpfcnpj);
						$pdvvenda->setcodfunc($codfunc);
						$pdvvenda->setseqecf($seqecf);
						$pdvvenda->setstatus($status);
						$pdvvenda->setchavecfe($chavecfe);
						$pdvvenda->setoperador($operador);
						$pdvvenda->pdvitem[] = $pdvitem;
						$this->pdvvenda[] = $pdvvenda;
					}
				}
			}

			// Finalizadora utilizada dentro do cupom fiscal
			foreach($M00->ZAN_M02S->ZAN_M02 AS $M02){
				if(strlen(trim($cpfcnpj)) == 0){
					$cpfcnpj = (string) $M02->M02BI;
				}
				$codfinaliz = (string) $M02->M02AI;
				$valortotal = (string) $M02->M02AK;
				$caixa = (string) $M02->M02ZB;
				$operador = (string) $M02->M02AH;				

				if($valortotal == 0){
					continue;
				}

				$finalizador = new PdvFinalizador();
				$finalizador->setstatus($status);
				$finalizador->setcupom($cupom);
				$finalizador->setcaixa($caixa);
				$finalizador->setdata($data);
				$finalizador->sethora($hora);
				$finalizador->setcodfinaliz($codfinaliz);
				$finalizador->setvalortotal($valortotal);				
				$finalizador->setcpfcliente($cpfcnpj);

				foreach($this->pdvvenda as $pdvvenda){
					if($pdvvenda->getdata() == $finalizador->getdata() && $pdvvenda->getcaixa() == $finalizador->getcaixa() && $pdvvenda->getcupom() == $finalizador->getcupom()){
						$achou = TRUE;
						$finalizador->setcodfunc($pdvvenda->getcodfunc());
						$pdvvenda->setoperador($operador);
						if(strlen($pdvvenda->getcpfcnpj()) == 0 && strlen($finalizador->getcpfcliente()) > 0){
							$pdvvenda->setcpfcnpj($finalizador->getcpfcliente());
						}elseif(strlen($pdvvenda->getcodcliente()) == 0 && strlen($finalizador->getcodcliente()) > 0){
							$pdvvenda->setcodcliente($finalizador->getcodcliente());
						}elseif(strlen($pdvvenda->getcpfcnpj()) > 0 && strlen($finalizador->getcpfcliente()) == 0){
							$finalizador->setcpfcliente($pdvvenda->getcpfcnpj());
						}						
					}
				}

				if($achou){
					$this->pdvfinalizador[] = $finalizador;
				}
			}

			// Finalizadora utilizada fora do cupom fiscal
			foreach($M00->ZAN_M04S->ZAN_M04 AS $M04){
				if(strlen(trim($cpfcnpj,"0")) == 0){
					$cpfcnpj = (string) $M04->M04BI;
				}
				if(strlen(trim($cpfcnpj,"0")) == 0){
					$cpfcnpj = (string) $M04->M04AL;
				}
				
				$codfinaliz = (string) $M04->M04AI;
				$valortotal = (string) $M04->M04AK;
				$caixa = (string) $M04->M04ZB;				

				if($valortotal == 0){
					continue;
				}

				$finalizador = new PdvFinalizador();
				$finalizador->setstatus($status);
				$finalizador->setcupom($cupom);
				$finalizador->setcaixa($caixa);
				$finalizador->setdata($data);
				$finalizador->sethora($hora);
				$finalizador->setcodfinaliz($codfinaliz);
				$finalizador->setvalortotal($valortotal);				
				$finalizador->setcpfcliente($cpfcnpj);

				foreach($this->pdvvenda as $pdvvenda){
					if($pdvvenda->getdata() == $finalizador->getdata() && $pdvvenda->getcaixa() == $finalizador->getcaixa() && $pdvvenda->getcupom() == $finalizador->getcupom()){
						$achou = TRUE;
						$finalizador->setcodfunc($pdvvenda->getcodfunc());						
						if(strlen($pdvvenda->getcpfcnpj()) == 0 && strlen($finalizador->getcpfcliente()) > 0){
							$pdvvenda->setcpfcnpj($finalizador->getcpfcliente());
						}elseif(strlen($pdvvenda->getcodcliente()) == 0 && strlen($finalizador->getcodcliente()) > 0){
							$pdvvenda->setcodcliente($finalizador->getcodcliente());
						}elseif(strlen($pdvvenda->getcpfcnpj()) > 0 && strlen($finalizador->getcpfcliente()) == 0){
							$finalizador->setcpfcliente($pdvvenda->getcpfcnpj());
						}
					}
				}

				if($achou){
					$this->pdvfinalizador[] = $finalizador;
				}
			}


			if(false){
				// Mapa Resumo 				
				foreach($M00->ZAN_M05S->ZAN_M05 AS $M05){
					die;
					if(!in_array($codestabelec.$data.$caixa,$arr_mapa)){
						$maparesumo = objectbytable("maparesumo", NULL, $this->con);
						$maparesumo->setcodestabelec($codestabelec);
						$maparesumo->setcaixa($caixa);
						$maparesumo->setnumeroecf($numeroecf);
						$maparesumo->setnummaparesumo(($paramfiscal->getnummaparesumo() == 0 ? 1 : $paramfiscal->getnummaparesumo()));
						$maparesumo->setdtmovto($datareduc);
						$maparesumo->setcodecf($ecf->getcodecf());
						if($cupominicial == 0){
							$sql_max = "SELECT MAX(operacaofim) as operacao FROM maparesumo WHERE caixa=".$caixa;
							$sql_max .= " AND codestabelec = ".$codestabelec;
							$qr = $this->con->query($sql_max);
							$arr_max = $qr->fetchAll(2);
							foreach($arr_max as $max){
								$cupominicial = $max["operacao"] + 1;
							}
						}
						$maparesumo->setoperacaoini($cupominicial);
						$maparesumo->setoperacaofim($contordem);
						$maparesumo->setgtinicial($gtinicial);
						$maparesumo->setgtfinal($gtfinal);
						$maparesumo->settotalbruto($totalbruto);
						$maparesumo->settotalcupomcancelado($totalcanc);
						$maparesumo->setcuponscancelados($cuponscancelados);
						$maparesumo->setitenscancelados($itenscancelados);
						$maparesumo->settotaldescontocupom($totaldescto);
						$maparesumo->settotalliquido($totalbruto - $totalcanc - $totaldescto);
						$maparesumo->setnumseriefabecf($serieecf);
						$maparesumo->setreiniciofim($contreinicio);
						$maparesumo->setnumeroreducoes($contreduc);

						if(!$maparesumo->save()){
							$this->con->rollback();
							return FALSE;
						}
						$codmaparesumo = $maparesumo->getcodmaparesumo();
						$arr_mapa[$codmaparesumo] = $codestabelec.$data.$caixa;

					}else{
						$codmaparesumo = array_search($codestabelec.$data.$caixa, $arr_mapa);
					}

					$maparesumoimposto = objectbytable("maparesumoimposto", NULL, $this->con);
					$maparesumoimposto->setcodmaparesumo(codmaparesumo);
					$maparesumoimposto->settptribicms($tributacao["tptribicms"]);
					$maparesumoimposto->setaliqicms($tributacao["aliqicms"]);
					$maparesumoimposto->settotalliquido($tributacao["totalliquido"]);
					$maparesumoimposto->settotalicms($tributacao["totalicms"]);
					if(!$maparesumoimposto->save()){
						$this->con->rollback();
						return FALSE;
					}

				}
			}
		}		
		return true;
	}

	public function marcarMovimentos(){
		setprogress(0, "Marcando Movimentos");

		$xml = new SimpleXMLElement("<xml></xml>");

		$ZMI = $xml->addChild("ZMI");
		$DATABASES = $ZMI->addChild("DATABASES");
		$DATABASE = $DATABASES->addChild("DATABASE");
		$DATABASE->addAttribute('AUTOCOMMIT_VALUE', '1000');
		$DATABASE->addAttribute('AUTOCOMMIT_ENABLED', '1');
		$DATABASE->addAttribute('HALTONERROR', '1');
		$DATABASE->addAttribute('NAME', 'MANAGER');
		$COMMANDS = $DATABASE->addChild("COMMANDS");
		$FUNCTION = $COMMANDS->addChild("FUNCTION");
		$MARCAR_MOVIMENTOS = $FUNCTION->addChild("MARCAR_MOVIMENTOS");

		$xml_movimento = "<MOVIMENTO>";

		foreach($this->arr_movimento as $movimento){
			$xml_movimento .= "<M00>";
			$xml_movimento .= "<M00ZA>{$movimento["M00ZA"]}</M00ZA>";
			$xml_movimento .= "<M00AF>{$movimento["M00AF"]}</M00AF>";
			$xml_movimento .= "<M00AC>{$movimento["M00AC"]}</M00AC>";
			$xml_movimento .= "<M00AD>{$movimento["M00AD"]}</M00AD>";
			$xml_movimento .= "<M00_CRO>{$movimento["M00_CRO"]}</M00_CRO>";
			$xml_movimento .= "<M00_TRN>{$movimento["M00_TRN"]}</M00_TRN>";
			$xml_movimento .= "<STATUS>OK</STATUS>";
			$xml_movimento .= "</M00>";
		}
		$xml_movimento .= "</MOVIMENTO>";
		$xml_movimento = htmlentities($xml_movimento);
		$MARCAR_MOVIMENTOS->addChild("MOVIMENTOS", $xml_movimento, null);

		$result = $this->service_soap($ZMI->asXML());

		if($result === FALSE){
			return FALSE;
		}else{
//			$this->leituraVendas(substr($movimento["M00AF"],0,10),"00:00:00");
//			$this->arr_movimento = array();
			return TRUE;
		}
	}

	public function importar_maparesumo(){
		$this->setconfigwebservice();

		$xml = new SimpleXMLElement("<xml></xml>");

		$ZMI = $xml->addChild("ZMI");
		$DATABASES = $ZMI->addChild("DATABASES");
		$DATABASE = $DATABASES->addChild("DATABASE");
		$COMMANDS = $DATABASE->addChild("COMMANDS");
		$FUNCTION = $COMMANDS->addChild("FUNCTION");
		$BUSCAR_MOVIMENTOS = $FUNCTION->addChild("SELECIONAR_MAPA_RESUMO");
		$BUSCAR_MOVIMENTOS->addChild("COD_LOJA", "1", null);
	
		$BUSCAR_MOVIMENTOS->addChild("DTA_INICIO", "2018-10-15T00:00:00", null);
		$BUSCAR_MOVIMENTOS->addChild("DTA_FIM", date("Y-m-d")."T23:59:59", null);	

		$result = $this->service_soap($ZMI->asXML());

		if($result === FALSE){
			return FALSE;
		}
		
		$log = new Log("zanthus-mapa");				
		$log->write((string) $ZMI->asXML());
		$log->write((string) $result->asXML());

		return true;
	}

	private function enviar_departamento(){
		setprogress(0, "Exportando Departamentos");
		$res = $this->con->query("SELECT coddepto, nome FROM departamento");
		$arr_dep = $res->fetchAll(2);

		$xml = new SimpleXMLElement("<xml></xml>");

		$ZMI = $xml->addChild("ZMI");
		$DATABASES = $ZMI->addChild("DATABASES");
		$DATABASE = $DATABASES->addChild("DATABASE");
		$COMMANDS = $DATABASE->addChild("COMMANDS");
		$REPLACE = $COMMANDS->addChild("REPLACE");
		foreach($arr_dep as $i => $row_dep){
			setprogress(($i + 1) / sizeof($arr_dep) * 100, "Exportando Departamentos: ".($i + 1)." de ".sizeof($arr_dep));

			if($row_dep["coddepto"] > 999){
				$row_dep["coddepto"] = substr($row_dep["coddepto"], -2) + 100;
			}


			$DEPARTAMENTOS = $REPLACE->addChild("DEPARTAMENTOS");
			$DEPARTAMENTO = $DEPARTAMENTOS->addChild("DEPARTAMENTO");

			$DEPARTAMENTO->addChild("COD_LOJA", $this->pdvconfig->getestabelecimento()->getcodestabelec(), null);
			$DEPARTAMENTO->addChild("COD_DEPARTAMENTO", $row_dep["coddepto"], null);
			$DEPARTAMENTO->addChild("DESCRICAO", utf8_encode($row_dep["nome"]), null);
			$DEPARTAMENTO->addChild("COD_TECLA", "255", null);
		}
		$result = $this->service_soap($ZMI->asXML(), "D");
		if($result === FALSE){
			return FALSE;
		}
		return TRUE;
	}

	private function enviar_produto(){
		setprogress(0, "Exportando Produtos");

		$query = "SELECT count(produto.codproduto) ";
		$query .= "FROM produto ";
		$query .= "INNER JOIN produtoestab ON (produto.codproduto = produtoestab.codproduto) ";
		$query .= "INNER JOIN piscofins ON (produto.codpiscofinssai = piscofins.codpiscofins) ";
		$query .= "INNER JOIN produtoean ON (produto.codproduto = produtoean.codproduto) ";
		$query .= "INNER JOIN classfiscal ON (produto.codcfpdv = classfiscal.codcf) ";
		$query .= "LEFT JOIN icmspdv ON (classfiscal.tptribicms = icmspdv.tipoicms AND classfiscal.aliqicms = icmspdv.aliqicms AND classfiscal.aliqredicms = icmspdv.redicms AND produtoestab.codestabelec = icmspdv.codestabelec) ";
		$query .= "INNER JOIN ncm ON (produto.idncm = ncm.idncm) ";
		$query .= "INNER JOIN embalagem ON (produto.codembalvda = embalagem.codembal) ";
		$query .= "INNER JOIN unidade ON (embalagem.codunidade = unidade.codunidade) ";
		$query .= "WHERE produtoestab.codestabelec = ".$this->pdvconfig->getestabelecimento()->getcodestabelec()." ";
		$query .= "	AND produto.foralinha = 'N' ";
		$query .= "	AND produtoestab.disponivel = 'S' ";
		if(param("ESTOQUE", "CARGAITEMCOMESTOQ", $this->con) == "S"){
			$query .= " AND produtoestab.sldatual > 0 ";
		}
		if($this->pdvconfig->produto_parcial()){
			$query .= "	AND ".$this->pdvconfig->produto_parcial_query();
		}

		$res = $this->con->query($query);
		$total_registros = $res->fetchColumn(0);

		$i = 0;
		$limit = 1000;
		$offset = 0;
		$temproduto = true;

		while($temproduto){
			unset($arr_pro);

			$query = "SELECT produto.codproduto, ".$this->pdvconfig->sql_descricao().", produto.pesado, produto.pesounid, produtoestab.precovrj, ";
			$query .= "produto.precovariavel, produto.coddepto, produtoestab.custotab, classfiscal.tptribicms, round(classfiscal.aliqicms,2) as aliqicms, round(classfiscal.aliqredicms,2) as aliqredicms, ";
			$query .= "replace(ncm.codigoncm,'.','') AS ncm, (CASE WHEN classfiscal.tptribicms = 'F' THEN 5405 ELSE 5102 END) AS cfop, ";
			$query .= "classfiscal.codcst, unidade.sigla as unidade, embalagem.quantidade AS quanttrib, ";
			$query .= "COALESCE(produto.aliqmedia,ncm.aliqmedia) AS aliqmedia, produtoean.codean, produtoestab.precovrjof, ";
			$query .= "  COALESCE((SELECT composicao.explosaoauto FROM composicao WHERE composicao.codproduto = produto.codproduto LIMIT 1),'N') AS explosao, ";
			$query .= "produto.vasilhame, produto.codvasilhame, produtoestab.qtdatacado, produtoestab.precoatc, ";
			$query .= "(SELECT codean from produtoean WHERE codproduto = produto.codvasilhame limit 1) AS codeanvasilhame, ";
			$query .= "piscofins.codcst as piscofinscst, icmspdv.infpdv, ";
			$query .= "piscofins.aliqpis, piscofins.aliqcofins ";
			$query .= "FROM produto ";
			$query .= "INNER JOIN produtoestab ON (produto.codproduto = produtoestab.codproduto) ";
			$query .= "INNER JOIN piscofins ON (produto.codpiscofinssai = piscofins.codpiscofins) ";
			$query .= "INNER JOIN produtoean ON (produto.codproduto = produtoean.codproduto) ";
			$query .= "INNER JOIN classfiscal ON (produto.codcfpdv = classfiscal.codcf) ";
			$query .= "LEFT JOIN icmspdv ON (classfiscal.tptribicms = icmspdv.tipoicms AND classfiscal.aliqicms = icmspdv.aliqicms AND classfiscal.aliqredicms = icmspdv.redicms AND produtoestab.codestabelec = icmspdv.codestabelec) ";
			$query .= "INNER JOIN ncm ON (produto.idncm = ncm.idncm) ";
			$query .= "INNER JOIN embalagem ON (produto.codembalvda = embalagem.codembal) ";
			$query .= "INNER JOIN unidade ON (embalagem.codunidade = unidade.codunidade) ";
			$query .= "WHERE produtoestab.codestabelec = ".$this->pdvconfig->getestabelecimento()->getcodestabelec()." ";
			$query .= "	AND produto.foralinha = 'N' ";
			$query .= "	AND produtoestab.disponivel = 'S' ";
			if(param("ESTOQUE", "CARGAITEMCOMESTOQ", $this->con) == "S"){
				$query .= " AND produtoestab.sldatual > 0 ";
			}
			if($this->pdvconfig->produto_parcial()){
				$query .= "	AND ".$this->pdvconfig->produto_parcial_query();
			}

			$query .= " order by produtoean.codean ";
			$query .= " LIMIT $limit ";
			$query .= "OFFSET $offset";

			$res = $this->con->query($query);
			$arr_pro = $res->fetchAll(2);

			if(count($arr_pro) == 0){
				$temproduto=false;
				header_remove('Set-Cookie');
				return true;
			}

			$xml = new SimpleXMLElement("<xml></xml>");
			$ZMI = $xml->addChild("ZMI");
			$DATABASES = $ZMI->addChild("DATABASES");
			$DATABASE = $DATABASES->addChild("DATABASE");
			$COMMANDS = $DATABASE->addChild("COMMANDS");
			$REPLACE = $COMMANDS->addChild("REPLACE");

			foreach($arr_pro as $produto){
				if(strlen($produto["infpdv"]) == 0){
					$_SESSION["ERROR"] = "Tributacao não encontrada para o produto <b>{$produto["codean"]}</b> <br>Tributação: <b>{$produto["tptribicms"]}</b><br>Aliquota: <b>{$produto["aliqicms"]}</b><br>Red ICMS:<b>{$produto["aliqredicms"]}</b> ";
					return false;
				}
				$i++;
				setprogress($i / $total_registros * 100, "Exportando Produtos: ".$i." de ".$total_registros, TRUE);

				$MERCADORIAS = $REPLACE->addChild("MERCADORIAS");
				$MERCADORIA = $MERCADORIAS->addChild("MERCADORIA");
				$MERCADORIA->addChild("COD_LOJA", $this->pdvconfig->getestabelecimento()->getcodestabelec(), null);
				$MERCADORIA->addChild("COD_MERCADORIA", $produto["codean"], null);
				$MERCADORIA->addChild("DESCRICAO", substr((utf8_encode($produto["descricao"])), 0, 50), null);
				$MERCADORIA->addChild("COD_DEPARTAMENTO", $produto["coddepto"], null);
				$MERCADORIA->addChild("COD_TRIBUTACAO_MERCADORIA", $produto["infpdv"], null);
				$MERCADORIA->addChild("INDICA_PESO_TARA", "0", null);
				$MERCADORIA->addChild("COD_SITUACAO_TRIBUTARIA", $produto["codcst"], null);
				$MERCADORIA->addChild("COD_SIT_TRIB_PIS", str_pad($produto["piscofinscst"], 3, "0", STR_PAD_LEFT), null);
				$MERCADORIA->addChild("COD_SIT_TRIB_COFINS", str_pad($produto["piscofinscst"], 3, "0", STR_PAD_LEFT), null);
				$MERCADORIA->addChild("COD_SIT_TRIB_ISS", null, null);
				$MERCADORIA->addChild("QUANTIDADE_EMBALAGEM", "1", null);
				$MERCADORIA->addChild("ISENTO_PIS_COFINS", null, null);
				$MERCADORIA->addChild("COD_NCM", $produto["ncm"], null);
				$MERCADORIA->addChild("UNIDADE_VENDA", utf8_encode($produto["unidade"]), null);
				$MERCADORIA->addChild("UNIDADE_TRIB", utf8_encode($produto["unidade"]), null);
				$MERCADORIA->addChild("QUANTIDADE_TRIB", "1", null);
				$MERCADORIA->addChild("PERC_TRIBUTOS_MUNICIPAL", $produto["aliqmedia"], null);
				$MERCADORIA->addChild("PERC_TRIBUTOS_FEDERAL", $produto["aliqmedia"], null);
				$MERCADORIA->addChild("ALIQUOTA_PIS", $produto["aliqpis"], null);
				$MERCADORIA->addChild("ALIQUOTA_COFINS", $produto["aliqcofins"], null);

				if($produto["vasilhame"] == "S"){
					$MERCADORIA->addChild("INDICA_TRATAMENTO_VASILHAME", "1", null);
					$MERCADORIA->addChild("CONTROLA_REPETICAO_PRODUTO", "1", null);
					//$MERCADORIA->addChild("PROIBE_VENDA_FRACIONADA", "1", null);
				}

				if($produto["pesado"] == "S"){
					$MERCADORIA->addChild("INDICA_MERC_PESO_VARIAVEL", "1", null);
					$MERCADORIA->addChild("PROIBE_VENDA_FRACIONADA", "0", null);
				}else{
					$MERCADORIA->addChild("INDICA_MERC_PESO_VARIAVEL", "0", null);
					$MERCADORIA->addChild("PROIBE_VENDA_FRACIONADA", "1", null);
				}

				if(strlen($produto["codvasilhame"]) > 0){
					$MERCADORIA->addChild("COD_MERCADORIA_ASSOCIADA", $produto["codeanvasilhame"], null);
				}

				$PRODUTOS = $REPLACE->addChild("PRODUTOS");
				$PRODUTO = $PRODUTOS->addChild("PRODUTO");
				$PRODUTO->addChild("COD_PRODUTO", $produto["codean"], null);
				$PRODUTO->addChild("FLG_RETIRA", "R", null);
				$PRODUTO->addChild("COD_ANP", "0", null);
				$PRODUTO->addChild("FLG_VDA_ASSIST", "S", null);

				$MERCADORIA_TIPO_VENDAS = $REPLACE->addChild("MERCADORIA_TIPO_VENDAS");
				$MERCADORIA_TIPO_VENDAS = $MERCADORIA_TIPO_VENDAS->addChild("MERCADORIA_TIPO_VENDA");
				$MERCADORIA_TIPO_VENDAS->addChild("USUARIO");
				$MERCADORIA_TIPO_VENDAS->addChild("COD_LOJA", $this->pdvconfig->getestabelecimento()->getcodestabelec(), null);
				$MERCADORIA_TIPO_VENDAS->addChild("COD_MERCADORIA", $produto["codean"], null);
				$MERCADORIA_TIPO_VENDAS->addChild("COD_TIPO_VENDA", "1", null);
				$MERCADORIA_TIPO_VENDAS->addChild("PRECO_UNITARIO", $produto["precovrjof"] > 0 ? $produto["precovrjof"] : $produto["precovrj"], null);

				if($produto["explosao"] == "S"){
					$query = "SELECT DISTINCT itcomposicao.codproduto AS itcodproduto, itcomposicao.quantidade, (CASE WHEN produtoestab.precovrjof > 0 THEN produtoestab.precovrjof ELSE produtoestab.precovrj END) AS precovrj, ";
					$query .= "(SELECT codean from produtoean where codproduto = itcomposicao.codproduto limit 1) AS codean ";
					$query .= "FROM itcomposicao ";
					$query .= "INNER JOIN produto ON (itcomposicao.codproduto = produto.codproduto) ";
					$query .= "INNER JOIN composicao ON (itcomposicao.codcomposicao = composicao.codcomposicao) ";
					$query .= "INNER JOIN produtoean ON (itcomposicao.codcomposicao = composicao.codcomposicao) ";
					$query .= "INNER JOIN produtoestab ON (itcomposicao.codproduto = produtoestab.codproduto AND produtoestab.codestabelec = {$this->pdvconfig->getestabelecimento()->getcodestabelec()}) ";
					$query .= "WHERE composicao.codproduto = {$produto["codproduto"]} ";

					$res = $this->con->query($query);
					$arr_composicao = $res->fetchAll(2);

					$total_precovrj = 0;
					foreach($arr_composicao as $composicao){
						$total_precovrj += $composicao["precovrj"] * $composicao["quantidade"];
					}

					$arr_composicao = array_sort($arr_composicao, "quantidade", SORT_DESC);

					foreach($arr_composicao as $composicao){
						if($total_precovrj == 0){
							continue;
						}
						$fator = ($produto["precovrj"] / $total_precovrj);

						$PRODUTO_DECOMPOSICAO_INSUMOS = $REPLACE->addChild("PRODUTO_DECOMPOSICAO_INSUMOS");
						$PRODUTO_DECOMPOSICAO_INSUMOS = $PRODUTO_DECOMPOSICAO_INSUMOS->addChild("PRODUTO_DECOMPOSICAO_INSUMO");
						$PRODUTO_DECOMPOSICAO_INSUMOS->addChild("COD_LOJA", $this->pdvconfig->getestabelecimento()->getcodestabelec(), null);
						$PRODUTO_DECOMPOSICAO_INSUMOS->addChild("COD_MERCADORIA", $produto["codean"], null);
						$PRODUTO_DECOMPOSICAO_INSUMOS->addChild("COD_INSUMO", $composicao["codean"], null);
						$PRODUTO_DECOMPOSICAO_INSUMOS->addChild("PERCENTUAL_INSUMO", $fator, null);
						$PRODUTO_DECOMPOSICAO_INSUMOS->addChild("QUANTIDADE_INSUMO", $composicao["quantidade"], null);
					}
				}
			}

			$aux_xml = (string) $ZMI->asXML();

			if(strlen($aux_xml) > 0){
				$result = $this->service_soap($aux_xml, "P");
				if($result === FALSE){
					return FALSE;
				}
			}

			$offset += $limit;
		}
		header_remove('Set-Cookie');
		return TRUE;
	}

	private function enviar_cliente(){
		$query = "SELECT cli.nome, cli.cpfcnpj, cli.enderres, cli.bairrores, cli.cepres, cli.foneres, cli.rgie, cli.tppessoa, ";
		$query .= "cli.sexo, cli.numerores, cli.complementores, cli.codcliente, cidade.nome AS cidade_nome, statuscliente.codstatus AS status, ";
		$query .= "cidade.codoficial, cli.codcidaderes, cli.codpaisres, ";
		$query .= "	cli.complementoent, cli.limite1, cli.debito1, cli.limite2, cli.debito2, ";
		$query .= "(SELECT nome FROM cliente WHERE codcliente = cli.codempresa LIMIT 1) AS nomeempresa, ";
		$query .= "cli.tipopreco, cli.descfixo, cli.ufres, cli.senha ";
		$query .= "FROM cliente cli ";
		$query .= "LEFT JOIN cidade ON (cli.codcidaderes = cidade.codcidade) ";
		$query .= "INNER JOIN statuscliente ON (cli.codstatus = statuscliente.codstatus) ";
		$query .= "LEFT JOIN clienteestab ON (cli.codcliente = clienteestab.codcliente) ";

		$where = array();
		if(param("CADASTRO", "MIXCLIENTE", $this->con) == "S"){
			$where[] = "clienteestab.codestabelec = ".$this->pdvconfig->getestabelecimento()->getcodestabelec()." ";
		}
		if($sincpdv){
			$where[] = "clienteestab.sincpdv IN (0, 1)";
		}
		if(count($where) > 0){
			$query .= "WHERE ".implode(" AND ", $where);
		}

		$res = $this->con->query($query);
		$arr_cliente = $res->fetchAll(2);

		$xml = new SimpleXMLElement("<xml></xml>");
		$ZMI = $xml->addChild("ZMI");
		$DATABASES = $ZMI->addChild("DATABASES");
		$DATABASE = $DATABASES->addChild("DATABASE");
		$COMMANDS = $DATABASE->addChild("COMMANDS");
		$REPLACE = $COMMANDS->addChild("REPLACE");
		$CLIENTES = $REPLACE->addChild("CLIENTES");

		foreach($arr_cliente as $i => $row){
			setprogress(($i + 1) / sizeof($arr_cliente) * 100, "Exportando Clientes: ".($i + 1)." de ".sizeof($arr_cliente));

			$CLIENTE = $CLIENTES->addChild("CLIENTE");

			$CLIENTE->addChild("COD_CLIENTE", removeformat($row["cpfcnpj"]), null);
			$CLIENTE->addChild("DES_CLIENTE", $row["nome"], null);
			$CLIENTE->addChild("NUM_CGC", removeformat($row["cpfcnpj"]), null);
			$CLIENTE->addChild("NUM_INSC_EST", removeformat($row["rgie"]), null);
			$CLIENTE->addChild("ID_MUNICIPIO", $row["codcidaderes"], null);
			$CLIENTE->addChild("COD_PAIS", $row["codpaisres"], null);

			$enderres = removespecial(utf8_decode($row["enderres"]));
			$CLIENTE->addChild("DES_ENDERECO", $enderres, null);
			$CLIENTE->addChild("NUMERO", $row["numerores"], null);
			$CLIENTE->addChild("COMPL_ENDERECO", $row["complementores"], null);
			$CLIENTE->addChild("DES_BAIRRO", $row["bairrores"], null);
			$CLIENTE->addChild("NUM_CEP", $row["cepres"], null);
			$CLIENTE->addChild("DES_CIDADE", $row["cidade_nome"], null);
			$CLIENTE->addChild("DES_SIGLA", $row["ufres"], null);
			$CLIENTE->addChild("VAL_LIMITE_CREDITO", $row["limite1"], null);
			$CLIENTE->addChild("VAL_DEBITO", $row["debito1"], null);
		}
		$result = $this->service_soap($ZMI->asXML(), "C");
		if($result === FALSE){
			$_SESSION["ERROR"] .= "\nCliente codcliente: ".$row["codcliente"];
			return FALSE;
		}

		return true;
	}

	private function zanthusfile_exportproduto($return = FALSE){
		// Busca os departamentos
		setprogress(0, "Buscando departamentos", TRUE);
		$linhas_departamento = array();
		$res = $this->con->query("SELECT coddepto, nome FROM departamento");
		$arr_dep = $res->fetchAll(2);
		foreach($arr_dep as $i => $row_dep){
			if($row_dep["coddepto"] > 999){
				$row_dep["coddepto"] = substr($row_dep["coddepto"],-2) + 100;
			}

			setprogress(($i + 1) / sizeof($arr_dep) * 100, "Exportando departamentos: ".($i + 1)." de ".sizeof($arr_dep));
			$linha_departamento = date("dmy"); // Data atual
			$linha_departamento .= "000"; // Controle (000 = Total; 008 = Alterados)
			$linha_departamento .= str_pad(substr($row_dep["coddepto"], 0, 3), 3, "0", STR_PAD_LEFT); // Codigo do departamento
			$linha_departamento .= str_pad(substr($row_dep["nome"], 0, 20), 20, " ", STR_PAD_RIGHT); // Descricao do departamento
			$linha_departamento .= str_pad(substr($row_dep["coddepto"], 0, 3), 3, "0", STR_PAD_LEFT); // Codigo do departamento
			$linha_departamento .= "000"; // Faixa de preco
			$linha_departamento .= "255"; // Codigo da tecla de balanca
			$linha_departamento .= "00"; // Tributacao
			$linha_departamento .= "0"; // Autenticacoes
			$linhas_departamento[] = $linha_departamento;
		}
		// Busca as informacoes de tributacoes
		$res = $this->con->query("SELECT * FROM icmspdv WHERE codestabelec = ".$this->estabelecimento->getcodestabelec());
		$arr_icmspdv = $res->fetchAll(2);
		// Busca os produtos
		setprogress(0, "Buscando produtos", TRUE);
		$linhas_produto = array();
		$query = "SELECT produto.codproduto, ".$this->pdvconfig->sql_descricao().", produto.pesado, produto.pesounid, ".$this->str_preco.", ";
		$query .= "produto.precovariavel, produto.coddepto, produtoestab.custotab, classfiscal.tptribicms, classfiscal.aliqicms, classfiscal.aliqredicms, ";
		$query .= "replace(ncm.codigoncm,'.','') AS ncm, (CASE WHEN classfiscal.tptribicms = 'F' THEN 5405 ELSE 5102 END) AS cfop, ";
		$query .= "classfiscal.codcst, unidade.sigla as unidade, embalagem.quantidade AS quanttrib, ";
		$query .= "COALESCE(produto.aliqmedia,ncm.aliqmedia) AS aliqmedia, produtoean.codean, produtoestab.precovrjof ";
		$query .= "FROM produto ";
		$query .= "INNER JOIN produtoestab ON (produto.codproduto = produtoestab.codproduto) ";
		$query .= "INNER JOIN produtoean ON (produto.codproduto = produtoean.codproduto) ";
		$query .= "INNER JOIN classfiscal ON (produto.codcfpdv = classfiscal.codcf) ";
		$query .= "LEFT JOIN icmspdv ON (classfiscal.tptribicms = icmspdv.tipoicms AND classfiscal.aliqicms = icmspdv.aliqicms AND classfiscal.aliqredicms = icmspdv.redicms AND produtoestab.codestabelec = icmspdv.codestabelec) ";
		$query .= "INNER JOIN ncm ON (produto.idncm = ncm.idncm) ";
		$query .= "INNER JOIN embalagem ON (produto.codembalvda = embalagem.codembal) ";
		$query .= "INNER JOIN unidade ON (embalagem.codunidade = unidade.codunidade) ";
		$query .= "WHERE produtoestab.codestabelec = ".$this->estabelecimento->getcodestabelec()." ";
		$query .= "	AND produto.foralinha = 'N' ";
		$query .= "	AND produtoestab.disponivel = 'S' ";
		if(param("ESTOQUE", "CARGAITEMCOMESTOQ", $this->con) == "S"){
			$query .= " AND produtoestab.sldatual > 0 ";
		}
		if(strlen($this->datalog) > 0 && strlen($this->horalog)){
			$query .= "	AND produto.codproduto IN (SELECT DISTINCT codproduto FROM logpreco WHERE data >= '".$this->datalog."' AND data <= '".$this->datalogfim."' AND hora >= '".$this->horalog."' AND hora <= '".$this->horalogfim."') ";
		}

		$res = $this->con->query($query);
		$arr_pro = $res->fetchAll(2);

		$arr_codproduto = array();
		foreach($arr_pro as $i => $row_pro){
			setprogress(($i + 1) / sizeof($arr_pro) * 100, "Exportando produtos: ".($i + 1)." de ".sizeof($arr_pro));

			// Acha a tributacao certa do produto
			foreach($arr_icmspdv as $j => $row_icmspdv){
				if($row_pro["tptribicms"] == $row_icmspdv["tipoicms"] && $row_icmspdv["aliqicms"] == $row_pro["aliqicms"]){
					if($row_icmspdv["redicms"] == "R"){
						if($row_icmspdv["redicms"] == $row_pro["aliqredicms"]){
							break;
						}
					}else{
						break;
					}
				}
			}

			// Acha a tributacao certa do produto
			$res = $this->con->query("SELECT * FROM icmspdv WHERE codestabelec = ".$this->estabelecimento->getcodestabelec()." AND redicms = ".$row_pro["aliqredicms"]);
			$arr_icmspdv = $res->fetchAll(2);

			$_icmspdv = "N";
			foreach($arr_icmspdv as $j => $row_icmspdv){
				if($row_pro["tptribicms"] == $row_icmspdv["tipoicms"] && $row_icmspdv["aliqicms"] == $row_pro["aliqicms"]){
					$_icmspdv = "S";
					if($row_icmspdv["tptribicms"] == "R"){
						if($row_icmspdv["redicms"] == $row_pro["aliqredicms"]){
							break;
						}
					}else{
						break;
					}
				}
			}
			// Verifica a tributacao no cadastro de PDV
			if($_icmspdv == "N"){
				echo messagebox("error", "", "N&atilde;o encontrado informa&ccedil;&otilde;es tributarias para o PDV do produto <b>".$row_pro["codproduto"]."</b>: \n\n <b>Tipo de Tributa&ccedil;&atilde;o</b> = ".$row_pro["tptribicms"]."\n<b>Aliquota </b> = ".$row_pro["aliqicms"]."\n <b>Aliquota de Redu&ccedil;&atilde;o</b> = ".$row_pro["aliqredicms"]."\n\n <a onclick=\"openProgram('InfTribPDV')\">Clique aqui</a> para abrir o cadastro de tributa&ccedil;&atilde;o do PDV.");
				die();
			}

			$linha_produto = date("dmy"); // Data atual
			$linha_produto .= "000"; // Controle (000 = Total; 008 = Alterados)
			$linha_produto .= str_pad(substr(trim($row_pro["codean"]), 0, 17), 17, "0", STR_PAD_LEFT); // Codigo de barras
//			$descricaofiscal = substr(iconv("UTF-8", "UTF-8//IGNORE", utf8_encode($row_pro["descricaofiscal"])), 0, 44);
			$descricaofiscal = removespecial(substr(utf8_decode($row_pro["descricaofiscal"]), 0, 44));
			$linha_produto .= str_pad($descricaofiscal, 44, " ", STR_PAD_RIGHT); // Descricao
			$linha_produto .= str_pad($row_pro["preco"] * 100, 11, "0", STR_PAD_LEFT); // Preco de venda

			if($row_pro["coddepto"] > 999){
				$row_pro["coddepto"] = substr($row_pro["coddepto"],-2) + 100;
			}
			$linha_produto .= str_pad(substr($row_pro["coddepto"], 0, 3), 3, "0", STR_PAD_LEFT); // Secao
			$linha_produto .= "0"; // numero autenticação da mercadoria
			$linha_produto .= str_pad(substr($row_icmspdv["infpdv"], 0, 2), 2, " ", STR_PAD_RIGHT); // Codigo de tributacao
			$linha_produto .= str_repeat("0", 5); // Desconto
			$linha_produto .= str_repeat("0", 5); // Porcentagem de comissão de vendedor para as vendas do tipo 1 desta mercadoria
			$linha_produto .= str_repeat("0", 5); // Porcentagem de comissão de vendedor para as vendas do tipo 2 desta mercadoria
			$linha_produto .= "000"; // Codigo da tecla da balanca
			$linha_produto .= str_repeat("0", 17); // Codigo associado
			$linha_produto .= str_repeat(($row_pro["precovariavel"] == "S" ? "1" : "0"), 1); // Indicador de preco indexado
			$linha_produto .= str_repeat("0", 1); // Controle de digitacao de quantidade
			$linha_produto .= str_repeat("0", 1); // Indicador de permissao de venda
			$linha_produto .= str_repeat("0", 1); // Controle de digitacao de quantidade
			$linha_produto .= str_repeat("0", 1); // Controle de digitacao da tecla Enter
			$linha_produto .= str_pad("0", 5, "0", STR_PAD_LEFT); // Percentual de reducao
			$linha_produto .= str_repeat(($row_pro["pesounid"] == "P" ? "1" : "0"), 1); // Produto de peso variavel
			$linha_produto .= str_repeat("0", 84); // [Campos que nao precisam ser preenchidos]
			$linha_produto .= str_pad(number_format($row_pro["custotab"], 2) * 100, 39, "0", STR_PAD_LEFT); // Custo
			$linha_produto .= str_pad(0, 52, "0", STR_PAD_LEFT); // Preco de venda
			$linha_produto .= str_pad(0, 11, "0", STR_PAD_LEFT); // Preco de venda
			$linha_produto .= str_pad("", 123, "0");
			$linha_produto .= str_pad($row_pro["ncm"], 9, "0", STR_PAD_LEFT); // ncm
			$linha_produto .= str_pad("", 41, "0");
			$linha_produto .= str_pad((int) $row_pro["aliqicms"], 2, "0", STR_PAD_LEFT);
			$linha_produto .= str_pad("", 61, "0");
			$linha_produto .= str_pad(0, 7, "0");
			$linha_produto .= str_pad("", 96, "0");
			$linha_produto .= str_pad($row_pro["cfop"], 4, "0", STR_PAD_LEFT); // cfop
			$linha_produto .= str_pad($row_pro["codcst"], 4, "0", STR_PAD_RIGHT); // cst
			$linha_produto .= str_pad("", 122, "0");
			$linha_produto .= str_pad($row_pro["unidade"], 2, "0"); // unidade de venda
			$linha_produto .= str_pad("", 110, "0");
			$linha_produto .= str_pad(removeformat(round($row_pro["aliqmedia"], 2)), 5, "0", STR_PAD_LEFT);
			$linhas_produto[] = $linha_produto;
			$arr_codproduto[] = $row_pro["codproduto"];
		}

		$linhas_fiscal = $this->zanthus->exportar_produto_fiscal($arr_codproduto);

		if($return){
			return array(
				$this->file_create("5.sdf", $linhas_departamento, "w+", TRUE),
				$this->file_create("6.sdf", $linhas_produto, "w+", TRUE),
				$this->file_create("REGRAS_ICMS.csv", $linhas_fiscal, "w+", TRUE)
			);
		}else{
			$this->file_create("5.sdf", $linhas_departamento);
			$this->file_create("6.sdf", $linhas_produto);
			$this->file_create("REGRAS_ICMS.csv", $linhas_fiscal);
		}
	}

}

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import javax.swing.JOptionPane;
import java.io.BufferedOutputStream;

public class Docu856 {
	static ArrayList<String> arrayList = new ArrayList<>();
	static int count = 0;
	static StringBuilder sb = new StringBuilder();

	private static String Nullcheck(String value) {
		if (value == null) {
			return "";
		} else {
			count++;
			sb.append("\n");
			return value;
		}

	}

	public static void main(String[] args) throws IOException {
		BufferedOutputStream Docu856 = null;
		String Filename = "Document856_";

		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection("jdbc:mysql://52.202.240.75:3306/sewonedi", "wenestimadm",
					"wenestimadm38$!");

			String header_array = "select ST02 from sewonedi.header_f856;";
			Statement header_st = con.createStatement();

			ResultSet rs_header = header_st.executeQuery(header_array);

			while (rs_header.next()) {
				String obj = rs_header.getString("ST02");
				arrayList.add(obj);
			}
			rs_header.close();
			
			for (int i = 0; i < arrayList.size(); i++) {
				
				System.out.println(i);
				count=0;
				sb.setLength(0);	
				
				int shipment_EX = 0;
				int total_EX = 0;
				
				String EXIST_S = "SELECT EXISTS (select * from shipment_f856 where ST02='";
				EXIST_S +=arrayList.get(i);
				EXIST_S +="')as success;";
				
				String EXIST_O = "SELECT EXISTS (select * from order_f856 where ST02='";
				EXIST_O +=arrayList.get(i);
				EXIST_O +="')as success;";
				
				Statement st6 = con.createStatement();
				ResultSet rs6 = st6.executeQuery(EXIST_S);
				
				Statement st7 = con.createStatement();
				ResultSet rs7 = st7.executeQuery(EXIST_O);
				
				//Validation
				while(rs6.next()) {
					if(rs6.getInt("success")==0) {
						total_EX=0;
					}else {
						shipment_EX=1;
						while(rs7.next()) {
							if(rs7.getInt("success")==0) {
								total_EX=0;
							}else {
								total_EX=1;
							}
						}
					}
				}//rs6
				
				if(total_EX==0) { 
					String MSG="Document ST02 ";
					MSG +=arrayList.get(i);
					MSG +=" Creation failed";
					JOptionPane.showMessageDialog(null,MSG);
				
				}else {
					System.out.println("Document will be created");
					
					String filename_num = Filename.concat(arrayList.get(i));
					filename_num = filename_num.concat(".txt");
					System.out.println(filename_num);
					Docu856 = new BufferedOutputStream(new FileOutputStream(filename_num));
					
					String header_856 = "";
					header_856 += "	SELECT CONCAT('ST','~',ST01,'~',ST02) AS ST, \n";
					header_856 += "		   CONCAT_WS('~','BSN',BSN01,BSN02,BSN03,BSN04)AS BSN, \n";
					header_856 += "		   CONCAT('DTM','~',DTM01,'~',DTM02,'~',DTM03,'~',DTM04)AS DTM";
					header_856 += " from header_f856 ";
					header_856 += "WHERE ST02='";
					header_856 += arrayList.get(i);
					header_856 += "';";
					System.out.println(header_856);

					Statement st = con.createStatement();
					ResultSet rs = st.executeQuery(header_856);
					
					while(rs.next()) { System.out.println("RS1");
					  sb.append(Nullcheck(rs.getString("ST")));
					  sb.append(Nullcheck(rs.getString("BSN")));
					  sb.append(Nullcheck(rs.getString("DTM")));
					  sb.append("\n");
					  }//while()
					st.close();
					
					String shipment_856 = "";
					shipment_856 += "SELECT concat('HL','~',S_HL01,'~~',S_HL03)AS HL, \n";
					shipment_856 += "		concat_WS('~','MEA',G_MEA01,'G',G_MEA03,G_MEA04)AS G_MEA, \n";
					shipment_856 += "		concat_WS('~','MEA',N_MEA01,'N',N_MEA03,N_MEA04)AS N_MEA, \n";
					shipment_856 += "		concat('TD1','~',COALESCE(TD101,''),'~',COALESCE(TD102,''))AS TD1, \n";
					shipment_856 += "		concat_WS('~','TD5',TD501,TD502,TD503,TD504,TD505,'~',TD507,TD508)AS TD5,";
					shipment_856 += "		concat('TD3','~',COALESCE(TD301,'~'),'~',COALESCE(TD302,''),'~',COALESCE(TD303,''))AS TD3, \n";
					shipment_856 += "		concat('N1','~','ST','~~',ST_N103,'~',ST_N104)AS ST_N, \n";
					shipment_856 += "		concat('N1','~','SU','~~',SU_N103,'~',SU_N104)AS SU_N, \n";
					shipment_856 += "		concat('N1','~','SF','~~',SF_N103,'~',SF_N104)AS SF_N, \n";
					shipment_856 += "		concat('N1','~','MA','~~',MA_N103,'~',MA_N104)AS MA_N, \n";
					shipment_856 += " 		concat('REF','~','AO','~',AO_REF02)AS AO_REF, \n";
					shipment_856 += "  		concat('REF','~','AW','~',AW_REF02)AS AW_REF, \n";
					shipment_856 += "  		concat('REF','~','BM','~',BM_REF02)AS BM_REF, \n";
					shipment_856 += "		concat('REF','~','CN','~',CN_REF02)AS CN_REF, \n";
					shipment_856 += "		concat('REF','~','FR','~',FR_REF02)AS FR_REF, \n";
					shipment_856 += "		concat('REF','~','MB','~',MB_REF02)AS MB_REF, \n";
					shipment_856 += " 		concat('REF','~','PK','~',PK_REF02)AS PK_REF, \n";
					shipment_856 += "		concat('REF','~','SN','~',SN_REF02)AS SN_REF, \n";
					shipment_856 += "		concat('REF','~','DK','~',DK_REF02)AS DK_REF \n";
					shipment_856 += "		from shipment_f856";
					shipment_856 += "		WHERE ST02='";
					shipment_856 +=arrayList.get(i);
					shipment_856 +="';";
					System.out.println(shipment_856);
					
					Statement st2 = con.createStatement();
					ResultSet rs2 = st2.executeQuery(shipment_856);

					while (rs2.next()) {
						System.out.println("RS2");
						sb.append(Nullcheck((rs2.getString("HL"))));
						sb.append(Nullcheck((rs2.getString("G_MEA"))));
						sb.append(Nullcheck((rs2.getString("N_MEA"))));
						sb.append(Nullcheck((rs2.getString("TD1"))));
						sb.append(Nullcheck((rs2.getString("TD5"))));
						sb.append(Nullcheck((rs2.getString("TD3"))));
						sb.append(Nullcheck(rs2.getString("ST_N")));
						sb.append(Nullcheck((rs2.getString("SU_N"))));
						sb.append(Nullcheck((rs2.getString("SF_N"))));
						sb.append(Nullcheck((rs2.getString("MA_N"))));
						sb.append(Nullcheck((rs2.getString("AO_REF"))));
						sb.append(Nullcheck((rs2.getString("AW_REF"))));
						sb.append(Nullcheck((rs2.getString("BM_REF"))));
						sb.append(Nullcheck((rs2.getString("CN_REF"))));
						sb.append(Nullcheck((rs2.getString("FR_REF"))));
						sb.append(Nullcheck((rs2.getString("MB_REF"))));
						sb.append(Nullcheck((rs2.getString("PK_REF"))));
						sb.append(Nullcheck((rs2.getString("SN_REF"))));
						sb.append(Nullcheck((rs2.getString("DK_REF"))));
						sb.append("\n");
					}
					st2.close();
					
					String order_f856 = "";
					order_f856 += "SELECT CONCAT_WS('~','HL',O_HL01,O_HL02,O_HL03)AS O_HL, \n";
					order_f856 += "CONCAT_WS('~','LIN~',LIN02,LIN03,LIN04,LIN05,LIN06,LIN07)AS LIN, \n";
					order_f856 += "CONCAT_WS('~','SN1~',SN102,SN103,SN104)AS SN1, \n";
					order_f856 += "CONCAT('PRF','~',PRF01)AS PRF \n ";
					order_f856 += "from order_f856";
					order_f856 += "		WHERE ST02='";
					order_f856 +=arrayList.get(i);
					order_f856 +="';";
					System.out.println(order_f856);
					Statement st3 = con.createStatement();
					ResultSet rs3 = st3.executeQuery(order_f856);

					while (rs3.next()) {
						sb.append(Nullcheck(rs3.getString("O_HL")));
						sb.append(Nullcheck(rs3.getString("LIN")));
						sb.append(Nullcheck(rs3.getString("SN1")));
						sb.append(Nullcheck(rs3.getString("PRF")));
						sb.append("\n");
					}
					
					st3.close();
					String qu = "SELECT O_HL01 from sewonedi.order_f856 WHERE ST02='";
					qu+=arrayList.get(i);
					qu+="'order by O_HL01 desc limit 1;";
					
					String SE = "SELECT ST02 AS SE from order_f856 order by SE desc limit 1;";
					
					Statement st4 = con.createStatement();
					Statement st5 = con.createStatement();
					
					ResultSet rs4 = st4.executeQuery(qu);
					ResultSet rs5 = st5.executeQuery(SE);
										
					while (rs4.next()) {
						sb.append("\n");
						sb.append("CTT~");
						sb.append((rs4.getString("O_HL01")));
						sb.append("\n");
					}
					st4.close();
					while (rs5.next()) {
						System.out.println("RS5");
						sb.append("SE~");
						sb.append(count + 2);
						sb.append("~");
						sb.append((rs5.getString("SE")));
						
					}	
					System.out.println(sb);
					st5.close();
					
					Docu856.write(sb.toString().getBytes());
					Docu856.close();
	
				}//else
									
			} // for
	
		} catch (Exception e) {
			e.getStackTrace();
		} finally {
			Docu856.close();
			System.out.println("DONE");
		}
	}

}

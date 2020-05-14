import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

public class Document810 {
	static String ST_02;
	static char FS_ASCHII = 28;
	static String delimeter = "";
	static int FSnum = 0;
	static String FSNum;
	private static String Nullcheck(String value) {
		if (value ==null) {
			return "";
		}else {
			return value;
		}

	}

	private static void ST_02(String string) {
		ST_02 = string;		
	}
	public static void main(String[] args) {
		File file = new File("Docu810.txt");
		FileWriter writer = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection("***********************");
			
			String qu = "select * from header_f810;";
			String qu2 = "select * from detail_f810;";
			String qu3 = "select count(IT101) from********.detail_f810;";
			
			Statement st = con.createStatement();
			Statement st2 = con.createStatement();
			Statement st3 = con.createStatement();
			
			ResultSet rs = st.executeQuery(qu);
			ResultSet rs2 = st2.executeQuery(qu2);
			ResultSet rs3 = st3.executeQuery(qu3);

			writer = new FileWriter(file, false);
			while(rs.next()) {
				StringJoiner sj = new StringJoiner("~");
				sj.add("ST");sj.add(Nullcheck(rs.getString("ST01")));sj.add(Nullcheck(rs.getString("ST02")));							
				writer.write(sj.toString());
				writer.write(FS_ASCHII);FSnum++;
				
				StringJoiner BIG = new StringJoiner("~");
				BIG.add("BIG");BIG.add(rs.getString("BIG01"));BIG.add(rs.getString("BIG02"));BIG.add(delimeter);BIG.add(delimeter);BIG.add(delimeter);BIG.add(delimeter);BIG.add(rs.getString("BIG07"));
				writer.write(BIG.toString());
				writer.write(FS_ASCHII);FSnum++;
				
				StringJoiner CUR = new StringJoiner("~");
				CUR.add("CUR");CUR.add(Nullcheck(rs.getString("CUR01")));CUR.add(Nullcheck(rs.getString("CUR02")));
				writer.write(CUR.toString());
				writer.write(FS_ASCHII);FSnum++;
				
				StringJoiner REFCA = new StringJoiner("~");
				REFCA.add("REF");REFCA.add(Nullcheck("CA"));REFCA.add(Nullcheck(rs.getString("CA_REF02")));
				writer.write(REFCA.toString());
				writer.write(FS_ASCHII);FSnum++;
				
				StringJoiner REFDP = new StringJoiner("~");
				REFDP.add("REF");REFDP.add(Nullcheck("DP"));REFDP.add(Nullcheck(rs.getString("DP_REF02")));
				writer.write(REFDP.toString());
				writer.write(FS_ASCHII);FSnum++;
				
				StringJoiner REFPE = new StringJoiner("~");
				REFPE.add("REF");REFPE.add(Nullcheck("PE"));REFPE.add(Nullcheck(rs.getString("PE_REF02")));
				writer.write(REFPE.toString());
				writer.write(FS_ASCHII);FSnum++;
				
				StringJoiner BT = new StringJoiner("~");
				BT.add("N1");BT.add("BT");BT.add(rs.getString("BT_N102"));BT.add(rs.getString("BT_N103"));BT.add(rs.getString("BT_N104"));
				
				StringJoiner SUN1 = new StringJoiner("~");
				SUN1.add("N1");SUN1.add("SU");SUN1.add(Nullcheck(rs.getString("SU_N102")));SUN1.add(Nullcheck(rs.getString("SU_N103")));SUN1.add(Nullcheck(rs.getString("SU_N104")));
				writer.write(SUN1.toString());
				writer.write(FS_ASCHII);FSnum++;
				
				StringJoiner STN1 = new StringJoiner("~");
				STN1.add("N1");STN1.add("ST");STN1.add(Nullcheck(rs.getString("ST_N102")));STN1.add(Nullcheck(rs.getString("ST_N103")));STN1.add(Nullcheck(rs.getString("ST_N104")));
				writer.write(STN1.toString());writer.write(FS_ASCHII);FSnum++;
				
				StringJoiner DTM = new StringJoiner("~");
				DTM.add("DTM");DTM.add(Nullcheck(rs.getString("DTM01")));DTM.add(Nullcheck(rs.getString("DTM02")));
				writer.write(DTM.toString());writer.write(FS_ASCHII);FSnum++;
				
				
				StringJoiner TDS = new StringJoiner("~");
				String TDS01 = Integer.toString(rs.getInt("TDS01"));
				TDS.add("TDS");TDS.add((TDS01));
				writer.write(TDS.toString());writer.write(FS_ASCHII);FSnum++;
				writer.flush();
			}

			st.close();
			System.out.println(rs2);
			
			while(rs2.next()) {		
				StringJoiner IT1 = new StringJoiner("~");
				IT1.add("IT1");IT1.add(Nullcheck(rs2.getString("IT101")));IT1.add(Nullcheck(rs2.getString("IT102")));IT1.add(Nullcheck(rs2.getString("IT103")));IT1.add(Nullcheck(rs2.getString("IT104")));IT1.add(delimeter);IT1.add(Nullcheck(rs2.getString("IT106")));IT1.add(Nullcheck(rs2.getString("IT107")));IT1.add(Nullcheck(rs2.getString("IT108")));IT1.add(Nullcheck(rs2.getString("IT109")));
				writer.write(IT1.toString());writer.write(FS_ASCHII);FSnum++;
				
				StringJoiner ITA = new StringJoiner("~");
				String ITA_07 = Integer.toString(rs2.getInt("ITA07"));
				ITA.add("ITA");ITA.add(Nullcheck(rs2.getString("ITA01")));ITA.add(delimeter);ITA.add(delimeter);ITA.add(Nullcheck(rs2.getString("ITA04")));ITA.add(Nullcheck(rs2.getString("ITA05")));ITA.add(delimeter);ITA.add(ITA_07);
				writer.write(ITA.toString());writer.write(FS_ASCHII);FSnum++;
				
				StringJoiner REF = new StringJoiner("~");
				REF.add("IT1");REF.add("PK");REF.add(Nullcheck(rs2.getString("PK_REF02")));
				writer.write(REF.toString());
				ST_02(rs2.getString("ST02"));
				writer.flush();
			}//while()
			st2.close();
			
			while(rs3.next()) {
				StringJoiner CTT = new StringJoiner("~");
				CTT.add("CTT");CTT.add(Nullcheck(rs3.getString("count(IT101)")));
				writer.write(CTT.toString());
				writer.write(FS_ASCHII);FSnum++;FSnum++;
				FSNum = Integer.toString(FSnum);
				StringJoiner SE = new StringJoiner("~");
				SE.add("SE");SE.add(FSNum);SE.add(ST_02);
				writer.write(SE.toString());
				writer.write(FS_ASCHII);
				writer.flush();
			}
			System.out.println("DONE");
		}catch (Exception e) {
			System.err.println("Got an exception");
			System.err.println(e.getMessage());
		} // catch

	}

}

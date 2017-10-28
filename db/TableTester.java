package db;
import org.junit.Test;

import java.util.*;

public class TableTester{
    public static void testPrintTable(){
        Table t1 = new Table();
        ArrayList<Integer> col1 = new ArrayList<>();
        ArrayList<Integer> col2 = new ArrayList<>();
        for(int i = 0; i < 4; i++){
            col1.add(i);
            col2.add(i + 5);
        }
        t1.insertColumn("x int", col1);
        t1.insertColumn("y int", col2);
        t1.printTable();
    }
    public static void testJoin(){
        Table t1 = new Table();
        ArrayList<Integer> col1 = new ArrayList<>();
        col1.addAll(Arrays.asList(new Integer[]{2, 8, 13}));
        ArrayList<Integer> col2 = new ArrayList<>();
        col2.addAll(Arrays.asList(new Integer[]{5, 3, 7}));
        t1.insertColumn("x int", col1);
        t1.insertColumn("y int", col2);
        t1.printTable();
        Table t2 = new Table();
        ArrayList<Integer> col3 = new ArrayList<>();
        col3.addAll(Arrays.asList(new Integer[]{2, 8, 10}));
        ArrayList<Integer> col4 = new ArrayList<>();
        col4.addAll(Arrays.asList(new Integer[]{4, 9, 1}));
        t2.insertColumn("x int", col3);
        t2.insertColumn("z int", col4);
        t2.printTable();
        Table t3 = Table.join(t1,t2);
        System.out.println(t3.printTable());
    }
    public static void testJoin2(){
        Table t1 = new Table();
        ArrayList<Integer> col1 = new ArrayList<>();
        col1.addAll(Arrays.asList(new Integer[]{2, 8}));
        ArrayList<Integer> col2 = new ArrayList<>();
        col2.addAll(Arrays.asList(new Integer[]{5, 3}));
        ArrayList<Integer> col3 = new ArrayList<>();
        col3.addAll(Arrays.asList(new Integer[]{4, 9}));
        t1.insertColumn("x int", col1);
        t1.insertColumn("y int", col2);
        t1.insertColumn("z int", col3);
        System.out.println(t1.printTable());
        Table t2 = new Table();
        ArrayList<Integer> col4 = new ArrayList<>();
        col4.addAll(Arrays.asList(new Integer[]{7, 2}));
        ArrayList<Integer> col5 = new ArrayList<>();
        col5.addAll(Arrays.asList(new Integer[]{0, 8}));
        t2.insertColumn("a int", col4);
        t2.insertColumn("b int", col5);
        System.out.println(t2.printTable());
        Table t3 = Table.join(t1, t2);
        System.out.println(t3.printTable());
        System.out.println(t3.getNumRows());

    }
    /*public static void testAddRow(){
        Table t1 = new Table();
        ArrayList<Integer> col1 = new ArrayList<>();
        ArrayList<Integer> col2 = new ArrayList<>();
        for(int i = 0; i < 4; i++){
            col1.add(i);
            col2.add(i + 5);
        }
        t1.insertColumn("x int", col1);
        t1.insertColumn("y int", col2);
        Integer[] row = new Integer[]{1, 2};
        t1.addRow(row);
        t1.printTable();
    } */
    /*public static void testGetRow(){
        Table t1 = new Table();
        ArrayList<Integer> col1 = new ArrayList<>();
        ArrayList<Integer> col2 = new ArrayList<>();
        for(int i = 0; i < 4; i++){
            col1.add(i);
            col2.add(i + 5);
        }
        t1.insertColumn("x int", col1);
        t1.insertColumn("y int", col2);
        t1.printTable();
        for(int i = 0; i < 2; i++){
            System.out.print(t1.getRow(1)[i]);
        }
    }*/
    public static void dbTest() {

        Table t1 = new Table();
        ArrayList<Integer> col1 = new ArrayList<>();
        col1.addAll(Arrays.asList(new Integer[]{2, 8}));
        ArrayList<Integer> col2 = new ArrayList<>();
        col2.addAll(Arrays.asList(new Integer[]{5, 3}));
        ArrayList<Integer> col3 = new ArrayList<>();
        col3.addAll(Arrays.asList(new Integer[]{4, 9}));
        t1.insertColumn("x int", col1);
        t1.insertColumn("y int", col2);
        t1.insertColumn("z int", col3);
        System.out.println(t1.printTable());
        Table t2 = new Table();
        ArrayList<Integer> col4 = new ArrayList<>();
        col4.addAll(Arrays.asList(new Integer[]{7, 2}));
        ArrayList<Integer> col5 = new ArrayList<>();
        col5.addAll(Arrays.asList(new Integer[]{0, 8}));
        t2.insertColumn("a int", col4);
        t2.insertColumn("b int", col5);
        System.out.println(t2.printTable());
        t2.removeRow(1);
        System.out.println(t2.printTable());

    }
    public static void dbTest2(){
        Database db = new Database();
        System.out.println(db.transact("load tester1"));
        System.out.println(db.transact("load tester2"));
        System.out.println(db.transact("load tester4"));
        System.out.println(db.transact("create table tester3 as select x+y,x*y from tester1,tester2"));
    }
    public static void rtest(){
        Database db = new Database();
        System.out.println(db.transact("create table xyz (x int,y int,z int)"));
        System.out.println(db.transact("insert into xyz values 5,6,7"));
        System.out.println(db.transact("print xyz"));
        System.out.println(db.transact("load fans"));
        System.out.println(db.transact("print fans"));
        System.out.println(db.transact("load records"));
        System.out.println(db.transact("print records"));
        System.out.println(db.transact("select * from fans,records"));
        System.out.println(db.transact("load T1"));
        System.out.println(db.transact("load T2"));
    }

    public static void test(){
        Database db = new Database();
        System.out.println(db.transact("create table t0 (a int,b int,c int,d int)"));
        System.out.println(db.transact("insert into t0 values 1,2,3,4"));
        System.out.println(db.transact("insert into t0 values 4,3,5,6"));
        System.out.println(db.transact("insert into t0 values 15,17,3,3"));
        System.out.println(db.transact("print t0"));
        System.out.println(db.transact("select * from t0 where a < b and c == d"));
    }
    public static void joinMulti(){
        Database db = new Database();
        System.out.println(db.transact("create table t7 (x int,y int,z int,w int)"));
        System.out.println(db.transact("insert into t7 values 1,7,2,10"));
        System.out.println(db.transact("insert into t7 values 7,7,4,1"));
        System.out.println(db.transact("insert into t7 values 1,9,9,1"));
        System.out.println(db.transact("print t7"));
        System.out.println(db.transact("create table t8 (w int,b int,z int)"));
        System.out.println(db.transact("insert into t8 values 1,7,4"));
        System.out.println(db.transact("insert into t8 values 7,7,3"));
        System.out.println(db.transact("insert into t8 values 1,9,6"));
        System.out.println(db.transact("insert into t8 values 1,11,9"));
        System.out.println(db.transact("print t8"));
        System.out.println(db.transact("select x,y,z,w,b from t7,t8"));
    }
    public static void createSelectAdvanced(){
        Database db = new Database();
        System.out.println(db.transact("load teams"));
        System.out.println(db.transact("load fans"));
        System.out.println(db.transact("load records"));
        System.out.println(db.transact("create table t1 as select City,Season,Wins/Losses as Ratio from teams,records"));
        System.out.println(db.transact("create table t2 as select Firstname,Lastname,Mascot from fans,teams"));
        System.out.println(db.transact("create table t3 as select Lastname,City,Wins-Losses as Diff from fans,teams,records"));
        System.out.println(db.transact("print teams"));
        System.out.println(db.transact("print fans"));
        System.out.println(db.transact("select City,Season,Wins/Losses as Ratio from teams,records"));
    }
    public static void storeTest(){
        Database db = new Database();
        System.out.println(db.transact("create table t0 (a string,b string)"));
        System.out.println(db.transact("insert into t0 values '1','2'"));
        System.out.println(db.transact("load tester1"));

        System.out.println(db.transact("load tester2"));
        System.out.println(db.transact("load records"));
        System.out.println(db.transact("print records"));
        System.out.println(db.transact("select TeamName,Season,Wins,Losses from records where Wins >= Losses"));
    }
    public static void condition(){
        Database db = new Database();
        System.out.println(db.transact("load records"));
        System.out.println(db.transact("print records"));
        System.out.println(db.transact("select TeamName,Season,Wins,Losses from records where TeamName > 'Mets'"));
    }
    public static void malformedCom(){
        Database db = new Database();
        System.out.println(db.transact("select x- as y from t"));

    }
    public static void selectMix(){
        Database db = new Database();
        System.out.println(db.transact("create table mix (x int,y float, z float)"));
        System.out.println(db.transact("insert into mix values 1, 2.0, 3.0"));
        System.out.println(db.transact("insert into mix values -7, 30.7, 102.22"));
        System.out.println(db.transact("insert into mix values 99,3.24,0.05"));
        System.out.println(db.transact("print mix"));
        System.out.println(db.transact("select x+y as a, y*z as b from mix"));

    }
    public static void joinmulti(){
        Database db = new Database();
        System.out.println(db.transact("create table m1 (x string,y string, z float)"));
        System.out.println(db.transact("insert into m1 values 'tea', 'is', 10.000"));
        System.out.println(db.transact("insert into m1 values 'very', 'tasty', -99.999"));
        System.out.println(db.transact("insert into m1 values 'operating', 'systems', 23.32"));
        System.out.println(db.transact("insert into m1 values 'are', 'cool', 0.002"));
        System.out.println(db.transact("create table m2 (x string,y string, d int)"));
        System.out.println(db.transact("insert into m2 values 'tea', 'is', -3"));
        System.out.println(db.transact("insert into m2 values 'very', 'tasty', 70"));
        System.out.println(db.transact("insert into m2 values 'operating', 'systems', 1823"));
        System.out.println(db.transact("insert into m2 values 'are', 'cool', 909"));
        System.out.println(db.transact("print m1"));
        System.out.println(db.transact("select * from m1,m2"));

    }
    public static void testStr(){
        Database db = new Database();
        System.out.println(db.transact("create table condition (name string,age string, animal string)"));
        System.out.println(db.transact("insert into condition values 'john', 'seven', 'a'"));
        System.out.println(db.transact("insert into condition values 'Beck', 'eight', 'c'"));
        System.out.println(db.transact("insert into condition values 'holly', 'six', 'd'"));
        System.out.println(db.transact("create table condition2 (called string,number string, curse string)"));
        System.out.println(db.transact("insert into condition2 values 'michael', 'one', 'b'"));
        System.out.println(db.transact("insert into condition2 values 'snoopy', 'two', 'd'"));
        System.out.println(db.transact("insert into condition2 values 'harold', 'three', 'd'"));
        System.out.println(db.transact("select * from condition,condition2 where animal < curse"));
        System.out.println(db.transact("select * from condition,condition2 where animal > curse"));
        System.out.println(db.transact("select * from condition,condition2 where animal <= curse"));
        System.out.println(db.transact("select * from condition,condition2 where animal >= curse"));
    }
    public static void selectAdv(){
        Database db = new Database();
        System.out.println(db.transact("load fans"));
        System.out.println(db.transact("load teams"));
        System.out.println(db.transact("select Firstname,Lastname,Mascot from fans,teams"));
        System.out.println(("select Firstname,Lastname,Mascot from fans,teams where Mascot <= 'Pat Patriot' and Lastname < 'Ray'"));
    }

    public static void insNo(){
        Database db = new Database();
        System.out.println(db.transact("create table t (a int,b float,c int)"));

        System.out.println(db.transact("insert into t values 1, 1.0, NOVALUE"));
        System.out.println(db.transact("insert into t values 2, 2.0, NOVALUE"));
        System.out.println(db.transact("insert into t values 3, 3.0, NOVALUE"));
        System.out.println(db.transact("insert into t values 4, 4.0, NOVALUE"));
        System.out.println(db.transact("print t"));
        System.out.println(db.transact("select a*c as d, b/c as e from t"));

    }
    public static void hell(){
        Database db = new Database();
        System.out.println(db.transact("load records"));
        System.out.println(db.transact("load teams"));
        System.out.println(db.transact("load fans"));
        System.out.println(db.transact("load teamRecords"));
        System.out.println(db.transact("select TeamName,Sport,Season,Wins,Losses from teamRecords where TeamName > Sport and Wins > Losses"));
    }
    public static void selectNan(){
        Database db = new Database();
        System.out.println(db.transact("create table t (x int, y float)"));
        System.out.println(db.transact("insert into t values 1000,0.0"));
        System.out.println(db.transact("insert into t values 2147483647,0.0"));
        System.out.println(db.transact("insert into t values -2147483648,0.0"));
        System.out.println(db.transact("create table s as select x,y/y as y from t"));
        System.out.println(db.transact("select * from s where x < y"));
    }
    public static void selUn(){
        Database db = new Database();
        System.out.println(db.transact("create table natoPhoneticAlphabet (Letter string, CodeWord string)"));

        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'A', 'Alpha'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'C', 'Charlie'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'D', 'Delta'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'E', 'Echo'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'F', 'Foxtrot'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'G', 'Golf'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'H', 'Hotel'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'I', 'India'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'J', 'Julliett'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'K', 'Kilo'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'L', 'Lima'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'M', 'Mike'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'N', 'November'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'O', 'Oscar'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'P', 'Papa'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'Q', 'Quebec'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'R', 'Romeo'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'S', 'Sierra'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'T', 'Tango'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'U', 'Uniform'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'V', 'Victor'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'W', 'Whiskey'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'X', 'X-ray'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'Y', 'Yankee'"));
        System.out.println(db.transact("insert into natoPhoneticAlphabet values 'Z', 'Zulu'"));

        System.out.println(db.transact("select CodeWord,Letter from natoPhoneticAlphabet where CodeWord != 'Quebec'"));

    }
    public static void main(String[] args){
        hell();
    }
}
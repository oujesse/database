package db;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringJoiner;
import java.util.Arrays;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Database {
    private static final String REST = "\\s*(.*)\\s*";
    private static final String COMMA = "\\s*,\\s*";
    private static final String AND = "\\s+and\\s+";
    private static final Pattern CREATE_CMD = Pattern.compile("create table \\s*(.*)\\s*");
    private static final Pattern LOAD_CMD = Pattern.compile("load \\s*(.*)\\s*");
    private static final Pattern STORE_CMD = Pattern.compile("store \\s*(.*)\\s*");
    private static final Pattern DROP_CMD = Pattern.compile("drop table \\s*(.*)\\s*");
    private static final Pattern INSERT_CMD = Pattern.compile("insert into \\s*(.*)\\s*");
    private static final Pattern PRINT_CMD = Pattern.compile("print \\s*(.*)\\s*");
    private static final Pattern SELECT_CMD = Pattern.compile("select \\s*(.*)\\s*");
    private static final Pattern CREATE_NEW = Pattern.compile(
            "(\\S+)\\s+\\((\\S+\\s+\\S+\\s*(?:,\\s*\\S+\\s+\\S+\\s*)*)\\)");
    private static final Pattern SELECT_CLS =
            Pattern.compile("([^,]+?(?:,[^,]+?)*)\\s+from\\s+(\\S+\\s*(?:,\\s*\\S+\\s*)*)" + "" + ""
                    + "(?:\\s+where\\s+([\\w\\s+\\-*/\'<>=!]+?(?:\\s+and"
                    + "\\s+[\\w\\s+\\-*/\'<>=!]+?)*))?"
                    + "");
    private static final Pattern CREATE_SEL;
    private static final Pattern INSERT_CLS;
    private HashMap<String, Table> tablesMap;

    public Database() {
        tablesMap = new HashMap<String, Table>();
    }

    public String transact(String query) {
        System.out.println(query);
        Matcher m;
        if ((m = CREATE_CMD.matcher(query)).matches()) {
            return createTable(m.group(1));
        } else if ((m = LOAD_CMD.matcher(query)).matches()) {
            return loadTable(m.group(1));
        } else if ((m = STORE_CMD.matcher(query)).matches()) {
            return storeTable(m.group(1));
        } else if ((m = DROP_CMD.matcher(query)).matches()) {
            return dropTable(m.group(1));
        } else if ((m = INSERT_CMD.matcher(query)).matches()) {
            return insertRow(m.group(1));
        } else if ((m = PRINT_CMD.matcher(query)).matches()) {
            return printTable(m.group(1));
        } else if ((m = SELECT_CMD.matcher(query)).matches()) {
            return select(m.group(1));
        } else {
            return "ERROR: .*";
        }
    }

    private String createTable(String expr) {
        Matcher m;
        if ((m = CREATE_NEW.matcher(expr)).matches()) {
            return createNewTable(m.group(1), m.group(2).split(","));
        } else if ((m = CREATE_SEL.matcher(expr)).matches()) {
            return createSelectedTable(m.group(1), m.group(2), m.group(3), m.group(4));
        } else {
            return "ERROR: .*";
        }

    }

    private String createNewTable(String name, String[] cols) {
        Table T = new Table();
        T.name = name;
        StringJoiner joiner = new StringJoiner(",");

        for (int colSentence = 0; colSentence < cols.length; ++colSentence) {
            joiner.add(cols[colSentence]);
            String[] types2 = new String[]{"int", "string", "float"};
            ArrayList<String> typesAllowed = new ArrayList<>(Arrays.asList(types2));
            if (!typesAllowed.contains(separateExpr(cols[colSentence])[1])) {
                return "ERROR: .*";
            }
            if (T.quickAddHeader(cols[colSentence]).equals("")) {
                return "ERROR: .*";
            }
            T.table.put(separateExpr(cols[colSentence])[0], new ArrayList<String>());
        }
        tablesMap.put(name, T);
        return "";
    }

    private String createSelectedTable(String name, String exprs, String tables, String conds) {
        Table finTable = selectHelper(exprs, tables, conds);
        if (finTable.types.containsValue("error")) {
            return "ERROR: .*";
        }
        tablesMap.put(name, finTable);
        return "";
    }

    private static ArrayList<Object> convertStringToType(String[] lineTemp, Table t) {

        ArrayList<Object> line = new ArrayList<>();
        int counter = 0;
        for (String s : lineTemp) {
            if (counter >= t.headers.size()) {
                line.add(false);
                return line;
            }
            if (s.equals("NOVALUE") || s.equals("NaN")) {
                line.add(s);
            } else if (t.types.get(t.headers.get(counter)).equals("int")) {
                if (!(s.trim().matches("[-+]?\\d*\\.?\\d+")) || (s.contains("."))) {

                    line.add(false);
                } else {
                    Integer temp = Integer.parseInt(s.trim());
                    line.add(temp);
                }
            } else if (t.types.get(t.headers.get(counter)).equals("float")) {
                if (!(s.contains("."))) {

                    line.add(false);
                } else {
                    Float temp = Float.parseFloat(s);
                    line.add(temp);

                }
            } else if (t.types.get(t.headers.get(counter)).equals("string")) {

                if (s.matches("[-+]?\\d*\\.?\\d+")) {
                    line.add(false);
                } else {
                    line.add(s);
                }
            } else {
                if (s.charAt(0) != '\'' || s.charAt(s.length() - 1) != '\'' || s.length() <= 1) {
                    ArrayList<Object> returner = new ArrayList<>();
                    returner.add(false);
                    return returner;
                }
                line.add(s);
            }
            counter++;
        }
        return line;
    }

    private String[] trimpString(String[] lineTemp) {
        String[] fin = new String[lineTemp.length];
        int ind = 0;
        for (String s : lineTemp) {
            fin[ind] = s.trim();
            ind++;

        }
        return fin;
    }

    private String loadTable(String name) {
        try {
            File loaded = new File(name + ".tbl");
            Table T = new Table();
            T.name = name;
            Scanner scan2 = new Scanner(loaded);
            boolean isEmpt = true;
            if (scan2.hasNextLine()) {

                isEmpt = false;
            }
            if (isEmpt) {
                System.out.println("no line pls");
                return "ERROR: .*";
            }
            Scanner scan = new Scanner(loaded);
            String[] columnNames = scan.nextLine().split(",");
            for (String columnName : columnNames) {
                if (T.quickAddHeader(columnName).equals("")) {

                    return "ERROR: .*";
                }
                T.table.put(separateExpr(columnName)[0], new ArrayList());
            }
            System.out.println(T.headers.toString());
            while (scan.hasNext()) {
                String[] lineTemp = scan.nextLine().split(",");
                lineTemp = trimpString(lineTemp);
                System.out.println(Arrays.toString(lineTemp));
                ArrayList<Object> line = convertStringToType(lineTemp, T);
                if (line.contains(false)) {

                    return "ERROR: .*";
                }
                if (T.addRow(line).equals("ERROR: .*")) {

                    return "ERROR: .*";
                }
            }
            tablesMap.put(name, T);
            return "";
        } catch (FileNotFoundException e) {

            return "ERROR: .*";
        }
    }

    private String storeTable(String name) {
        if (!tablesMap.containsKey(name)) {
            return "ERROR: .*";
        }
        try {
            String content = "";
            File file = new File(name + ".tbl");
            file.createNewFile();
            BufferedWriter bw = new BufferedWriter(new FileWriter(file));
            int counter = 1;
            Table temp = tablesMap.get(name);
            for (String s : temp.headers) {
                if (counter == temp.headers.size()) {
                    content += s + " " + temp.types.get(s) + "\n";
                } else {
                    content += s + " " + temp.types.get(s) + ",";
                }
                counter += 1;
            }
            for (int i = 0; i < tablesMap.get(name).getNumRows(); i += 1) {
                int counter2 = 1;
                for (String s : temp.headers) {
                    String willAdd = "";
                    if (temp.types.get(s).equals("float")) {
                        willAdd = String.format("%.3f", temp.table.get(s).get(i));
                    } else {
                        willAdd = temp.table.get(s).get(i).toString();
                    }
                    if (counter2 == temp.headers.size()) {
                        content += willAdd + "\n";
                    } else {
                        content += willAdd + ",";
                    }
                    counter2 += 1;
                }
            }
            bw.write(content);
            bw.close();
            return "";
        } catch (IOException e) {
            return "Error: IOException";
        }
    }


    private String dropTable(String name) {
        if (tablesMap.containsKey(name)) {
            tablesMap.remove(name);
            return "";
        } else {
            return "ERROR: table does not exist";
        }
    }

    private String insertRow(String expr) {
        Matcher m = INSERT_CLS.matcher(expr);
        if (!m.matches()) {

            return "ERROR: .*";
        } else {
            Scanner scan = new Scanner(m.group(2));
            String[] line = scan.nextLine().split(",");
            if (tablesMap.get(m.group(1)) == (null)) {

                return "ERROR: .*";
            }

            ArrayList<Object> newLine =
                    convertStringToType(trimpString(line), tablesMap.get(m.group(1)));

            if (newLine.contains(false)) {

                return "ERROR: .*";
            }

            tablesMap.get(m.group(1)).addRow(newLine);
            return "";
        }
    }


    private String printTable(String name) {
        if (!tablesMap.containsKey(name)) {
            return "ERROR: .*";
        }
        return tablesMap.get(name).printTable();
    }

    private String select(String expr) {
        Matcher m = SELECT_CLS.matcher(expr);
        if (!m.matches()) {
            return "ERROR: .*";
        } else {
            return select(m.group(1), m.group(2), m.group(3));
        }
    }

    private String select(String exprs, String tables, String conds) {
        Table finTable = selectHelper(exprs, tables, conds);
        if (finTable.types.containsValue("error")) {
            return "ERROR: .*";
        }
        return finTable.printTable();
    }

    private boolean checkConds(String exprs, String tables, String conds,
                               ArrayList<ArrayList<String>> listConds) {
        boolean containsCond = false;
        if (conds != null) { //checks to see if there is a conditional portion of the command
            Scanner scanCond = new Scanner(conds);
            String[] listStringConds = scanCond.nextLine().split(",");

            containsCond = true;

            //forloop that takes the listStringConds and changes them to be the listConds
            for (String s : listStringConds) {
                ArrayList<String> temp = new ArrayList<>();

                ArrayList<Integer> andIndices = new ArrayList<>();
                for (int i = 0; i < s.length() - 4; i++) {
                    if (s.length() >= 5 && s.charAt(i) == ' ' && s.charAt(i + 1)
                            == 'a' && s.charAt(i + 2) == 'n' && s.charAt(i + 3)
                            == 'd' && s.charAt(i + 4) == ' ') {
                        andIndices.add(i);
                    }
                }
                ArrayList<Character> firstCompare = new ArrayList<>();
                firstCompare.add('>');
                firstCompare.add('<');
                firstCompare.add('=');
                firstCompare.add('!');
                ArrayList<Integer> comparisonIndices = new ArrayList<>();
                for (int i = 0; i < s.length(); i++) {
                    if (firstCompare.contains(s.charAt(i))) {
                        comparisonIndices.add(i);

                    }
                }
                int index = 0; //holds the index in Arraylist temp that is currently being added to
                for (int charInd = 0; charInd < s.length(); charInd++) {
                    if (andIndices.contains(0) || comparisonIndices.contains(0)) {
                        throw new RuntimeException("conditional starts with wrong character");
                    } else if (andIndices.contains(charInd)) { //checks the ' and '
                        index = 0;
                        listConds.add(temp);
                        temp = new ArrayList<>();
                        charInd += 4;
                    } else if (s.charAt(charInd) == ' ') {
                        charInd++;
                        charInd--;
                    } else if (comparisonIndices.contains(charInd)) {
                        index++;
                        temp.add(Character.toString(s.charAt(charInd)));
                        if (comparisonIndices.contains(charInd + 1)) {
                            temp.set(index, temp.get(index)
                                    + Character.toString(s.charAt(charInd + 1)));
                            charInd++;
                        }
                        index++;
                    } else {
                        if (temp.size() <= index) {
                            temp.add(index, Character.toString(s.charAt(charInd)));
                        } else {
                            temp.set(index, temp.get(index)
                                    + Character.toString(s.charAt(charInd)));
                        }
                    }
                }
                listConds.add(temp);
            }
        }
        return containsCond;
    }

    private String checkMath(String exprs, ArrayList<ArrayList<String>> listMathExpr,
                             String[] listExpr) {
        boolean containsMath = false;
        for (String s : listExpr) { //loops through to reformat listExpr into listMathExpr
            ArrayList<String> temp = new ArrayList<>();
            int index = 0; //holds the index that the Arraylist temp is currently adding to
            boolean starFirst = true;
            int asInd = 0;
            boolean hasAs = false;
            for (int j = 0; j < s.length() - 3; j++) { //loops thru to find the index of ' as '
                if (s.charAt(j) == ' ' && s.charAt(j + 1) == 'a'
                        && s.charAt(j + 2) == 's' && s.charAt(j + 3) == ' ') {
                    hasAs = true;
                    asInd = j;
                }
            }

            for (int charInd = 0; charInd < s.length(); charInd++) {
                if (charInd == asInd && hasAs) { //checks if we're at ' as '
                    index++;
                    temp.add("as");
                    index++;
                    if (!containsMath) {
                        throw new RuntimeException("has an ' as ' but has no math operator");
                    }
                    charInd += 3; //unsure if i incremented this right
                } else if (s.charAt(charInd) == ' ') {
                    charInd++;
                    charInd--;
                } else if (index == 0 && s.charAt(charInd) == '*' && starFirst) {
                    temp.add(0, Character.toString(s.charAt(charInd)));
                    if (s.trim().length() > 1) { //makes sure there's nothing that comes after '*'
                        throw new RuntimeException("your expression is invalid: " + exprs);
                    }
                } else if (s.charAt(charInd) == '+' || s.charAt(charInd) == '/'
                        || s.charAt(charInd) == '-' || s.charAt(charInd) == '*') {
                    containsMath = true;
                    index++;
                    if (index != 1) {
                        throw new RuntimeException("your math operator"
                                + " is not in between 2 columnNames");
                    }
                    temp.add(index, Character.toString(s.charAt(charInd)));
                    index++;
                } else {

                    if (temp.size() <= index) {
                        if (s.length() <= charInd) {
                            return "ERROR: .*";
                        }
                        temp.add(Character.toString(s.charAt(charInd)));
                        starFirst = false;
                    } else {
                        temp.set(index, temp.get(index) + Character.toString(s.charAt(charInd)));
                    }
                }
            }
            listMathExpr.add(temp);
        }
        if (containsMath) {
            return "true";
        }
        return "false";
    }

    private Table doesContainMath(boolean containsMath, boolean containsCond, Table[] actualTables,
                                  ArrayList<ArrayList<String>> listMathExpr,
                                  ArrayList<ArrayList<String>> listConds) {
        if (containsMath) {
            if (containsCond) {
                return Table.evalCond2(Table.evalMath(actualTables, listMathExpr), listConds);
            }
            for (int k = 0; k < listMathExpr.size(); k++) {
                if (listMathExpr.get(k).size() != 5 && listMathExpr.get(k).size() != 1) {
                    System.out.println("You printed: " + listMathExpr);
                    Table badInput = new Table();
                    badInput.types.put("ERRORcode", "error");
                    return badInput;
                }
            }
            return Table.evalMath(actualTables, listMathExpr);
        }
        Table noOut = new Table();
        noOut.types.put("hiya", "eyo");
        return noOut;
    }

    private Table selectHelper(String exprs, String tables, String conds) {
        Scanner scan = new Scanner(tables);
        String[] tableNames = scan.nextLine().split(",");
        Table[] actualTables = new Table[tableNames.length];
        for (int i = 0; i < tableNames.length; i++) {
            actualTables[i] = tablesMap.get(tableNames[i].trim());
        }
        Scanner scanExpr = new Scanner(exprs);
        String[] listExpr = scanExpr.nextLine().split(",");
        ArrayList<ArrayList<String>> listMathExpr = new ArrayList<>();
        ArrayList<ArrayList<String>> listConds = new ArrayList<>();
        boolean containsCond = checkConds(exprs, tables, conds, listConds);
        String containsMathCheck = checkMath(exprs, listMathExpr, listExpr);
        boolean containsMath;
        if (containsMathCheck.equals("ERROR: .*")) {
            Table badInput = new Table();
            badInput.types.put("ERRORcode", "error");
            return badInput;
        } else if (containsMathCheck.equals("true")) {
            containsMath = true;
        } else {
            containsMath = false;
        }
        Table doTheMath = doesContainMath(containsMath, containsCond, actualTables,
                listMathExpr, listConds);
        if (!doTheMath.types.containsValue("eyo")) {
            return doTheMath;
        }
        listExpr = trimpString(listExpr);
        Table returner = new Table();
        if (tableNames.length == 1) { //checks if from only uses 1 table
            Table returnTable = selSingleTable(tableNames, listExpr, containsCond,
                    listConds, returner);
            if (!returnTable.types.containsValue("eyo")) {
                return returnTable;
            }
        } else { //the case where there are multiple tables
            Table temp = tablesMap.get(tableNames[0]);
            for (int i = 1; i < tableNames.length; i++) {
                temp = Table.join(temp, tablesMap.get(tableNames[i]));
            }
            if (listExpr[0].equals("*")) {
                if (containsCond) {
                    return Table.evalCond2(temp, listConds);
                }
                return temp;
            } else {
                if (containsCond) {
                    temp = Table.evalCond2(temp, listConds);
                }
                Table dummy = temp.makeClone();
                for (String s : temp.headers) {
                    ArrayList<String> listExprArray =
                            new ArrayList<>(Arrays.asList(listExpr));
                    if (!listExprArray.contains(s)) {
                        if (dummy.removeColumn(s.trim() + " "
                                + dummy.types.get(s.trim())).equals("ERROR: .*")) {
                            return returnBadInput();
                        }
                    }
                }
                return makeArrangedTable(dummy, listExpr);
            }
        }
        return returner;
    }

    private static Table makeArrangedTable(Table old, String[] listExpr) {
        Table newTable = old.makeClone();
        for (String s : listExpr) {
            newTable.removeColumn(s + " " + old.types.get(s));
        }

        for (String s : listExpr) {
            newTable.insertColumn(s + " " + old.types.get(s), old.table.get(s));
        }
        for (String s : old.headers) {
            ArrayList<String> arrayListExp = new ArrayList<>(Arrays.asList(listExpr));
            if (!arrayListExp.contains(s)) {
                newTable.removeColumn(s + " " + old.types.get(s));
            }
        }
        return newTable;
    }

    private static Table returnBadInput() {
        Table badInput = new Table();
        badInput.types.put("ERRORcode", "error");
        return badInput;
    }

    private Table selSingleTable(String[] tableNames, String[] listExpr, boolean containsCond,
                                 ArrayList<ArrayList<String>> listConds, Table returner) {
        Table temp = tablesMap.get(tableNames[0]);
        if (listExpr[0].equals("*")) {
            if (containsCond) {
                return Table.evalCond2(temp, listConds);
            }
            return temp;
        }
        for (String s : listExpr) {
            if (temp.table.containsKey(s)) {
                if (returner.insertColumn(s + " " + temp.types.get(s),
                        temp.table.get(s)).equals("ERROR: .*")) {
                    Table badInput = new Table();
                    badInput.types.put("ERRORcode", "error");
                    return badInput;
                }
            }
        }
        if (containsCond) {
            return makeArrangedTable(Table.evalCond2(temp, listConds), listExpr);
        }
        Table noPut = new Table();
        noPut.types.put("hiya", "eyo");
        return noPut;
    }

    private static String[] separateExpr(String expr) { //helper method
        String name = "";
        String typing = "";
        Boolean isName = true;
        boolean detectedFirst = false;
        boolean detectedFirst2 = false;
        for (char c : expr.toCharArray()) {
            if (isName) { //checks if it will add c to the name
                if (!detectedFirst) {
                    if (c != ' ') {
                        detectedFirst = true;
                        name += c;
                    }
                } else if (c == ' ') {
                    isName = false;
                } else {
                    name += c;
                }
            } else { //checks if it will add c to the typing
                if (!detectedFirst2) {
                    if (c != ' ') {
                        detectedFirst2 = true;
                        typing += c;
                    }
                } else {
                    typing += c;
                }
            }
        }
        if (typing.equals("")) {
            return new String[]{};
        }
        return new String[]{name, typing};
    }
    /*public static Object evalValue(String value){
        char[] valueChars = value.toCharArray();
        boolean isNum = true;
        boolean isFloat = true;
        for(char c : valueChars){
            if(c.)
        }
        return "aw";
    }*/

    static {
        CREATE_SEL = Pattern.compile("(\\S+)\\s+as select\\s+" + SELECT_CLS.pattern());
        INSERT_CLS = Pattern.compile("(\\S+)\\s+values\\s+(.+?\\s*(?:,\\s*.+?\\s*)*)");
    }
}

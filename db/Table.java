package db;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayDeque;
import java.util.Arrays;

/**
 * Created by ryanleung on 2/27/17.
 */
public class Table {

    String name; //name of table
    ArrayList<String> headers; //holds the header names in the order they appear in the table
    Map<String, ArrayList> table; //maps each column to their headers
    Map<String, String> types; //maps each header name to its type

    //constructor
    public Table() {
        headers = new ArrayList<>();
        table = new HashMap<>();
        types = new HashMap<>();
    }

    public void removeRow(int i) {
        for (String header : headers) { //loops through each header and removes one value from the row to that column
            table.get(header).remove(i);
        }
    }

    public Table makeClone() {
        Table temp = new Table();
        temp.headers = new ArrayList<>();
        for (String s : this.headers) {
            temp.headers.add(s);
        }

        temp.name = this.name;
        for (Map.Entry<String, ArrayList> entry : this.table.entrySet()) {
            temp.table.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
        for (Map.Entry<String, String> entry : this.types.entrySet()) {
            temp.types.put(entry.getKey(), (entry.getValue()));
        }
        return temp;
    }
    public String removeColumn(String headerExpr){
        System.out.println(headerExpr);
        if (separateHeader(headerExpr).length == 0) {
            return "ERROR: .*";
        }
        if (!headers.contains(separateHeader(headerExpr)[0])) { //the if statement makes sure that a header is already in the table
            return "ERROR: .*";
        }
        table.remove(separateHeader(headerExpr)[0]);
        types.remove(separateHeader(headerExpr)[0]);
        headers.remove(separateHeader(headerExpr)[0]);
        return "";
    }
    //inserts a column into the table
    public String insertColumn(String headerExpr, ArrayList column) {
        if (separateHeader(headerExpr).length == 0) {
            return "ERROR: .*";
        }
        if (headers.contains(separateHeader(headerExpr)[0])) { //the if statement makes sure that a header isn't already in the table
            return "ERROR: .*";
        }

        if (quickAddHeader(headerExpr).equals("")) {
            return "ERROR: .*";
        }
        table.put(separateHeader(headerExpr)[0], column);
        return "";
    }

    public String quickAddHeader(String headerExpr) {
        if (separateHeader(headerExpr).length == 0) {
            return "";
        }
        headers.add(separateHeader(headerExpr)[0]);

        types.put(separateHeader(headerExpr)[0], separateHeader(headerExpr)[1]);
        return "good";
    }

    //converts "int" to "int" and "float" to "Float" and "string" to "String"
    public String interpretTypeAsObject(String typer) {
        if (typer.equals("int")) {
            return "Integer";
        }
        if (typer.equals("float")) {
            return "Float";
        } else {
            return "String";
        }
    }

    //prints the table's values
    public String printTable() {
        String result = "";
        if (headers.isEmpty()) {
            return "empty table";
        }
        int counter = 1;
        for (String s : headers) { //prints the header names first
            if (counter == headers.size()) {
                result += s + " " + types.get(s);
            } else {
                result += s + " " + types.get(s) + ",";
            }
            counter += 1;
        }
        result = result + "\n";
        //loops through each value in each column and prints them out
        //first forloop loops through each index of a column
        for (int i = 0; i < table.get(headers.get(0)).size(); i++) { //table.get will get the first column's size
            result += getRow(i); //prints the value at row i
            result += "\n";
        }
        return result;
    }

    //adds a generic array as a row to the table
    public String addRow(ArrayList values) {

        int index = 0;
        if (headers.size() != values.size()) { //makes sure that the length of the row is equal to the number of columns
            return "ERROR: .*";
        }
        for (String header : headers) { //loops through each header in order and adds one value from the row to that column
            if (!values.get(index).getClass().getName().equals("java.lang." + interpretTypeAsObject(types.get(header))) && !values.get(index).equals("NOVALUE") && !values.get(index).equals("NaN")) { //checks to see if each value's type matches with the column type
                throw new RuntimeException("Types do not match up to columns");
            }
            table.get(header).add(values.get(index));
            index++;
        }
        return "";
    }

    //returns the row as a string at an index in a table represented as a generic array
    public String getRow(int index) {
        String row = "";
        int counter = 1;
        for (String head : headers) {
            //might be problem with referencing an object in the column
            String willAdd = "";
            if (table.get(head).get(index).equals("NOVALUE")) {
                willAdd = "NOVALUE";
            } else if (table.get(head).get(index).equals("NaN")) {
                willAdd = "NaN";
            } else if ((types.get(head).equals("float"))) {
                willAdd = String.format("%.3f", table.get(head).get(index));
            } else {
                willAdd = table.get(head).get(index).toString();
            }
            if (counter == headers.size()) {
                row += willAdd;
            } else {
                row += willAdd + ",";
            }
            counter++;
        }

        return row;
    }

    private static String[] separateHeader(String head) { //helper method
        String name = "";
        String typing = "";
        Boolean isName = true;
        boolean detectedFirst = false;
        boolean detectedFirst2 = false;
        for (char c : head.toCharArray()) {
            if (isName) { //checks if it will add c to the name
                if (!detectedFirst) {
                    if (c != ' ') {
                        detectedFirst = true;
                        name += c;
                    }
                } else if (c == ' ') { //converts the loop to add to the typing once it hits the space
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

    //returns the row at an index in a table represented as a generic array
    public Object[] getRowObject(int index) {
        Object[] row = new Object[headers.size()];
        int counter = 0;
        for (String head : headers) {
            //might be problem with referencing an object in the column
            row[counter] = table.get(head).get(index);

            counter++;
        }

        return row;

    }

    public int getNumRows() {
        if (headers.size() == 0) {
            return 0;
        }
        return table.get(headers.get(0)).size();
    }

    public static Table join(Table[] tables) {
        Table result = tables[0];
        for (Table willJoin : tables) {
            result = join(result, willJoin);
        }
        return result;
    }


    public static Table evalMath(Table[] tables, ArrayList<ArrayList<String>> exprs) {

        Table joined = Table.join(tables);
        Table select = new Table();
        for (ArrayList<String> expr : exprs) {
            if (expr.size() == 1) {
                if (select.insertColumn(expr.get(0) + " " + joined.types.get(expr.get(0)), joined.table.get(expr.get(0))).equals("ERROR: .*")) {

                    Table badInput = new Table();
                    badInput.types.put("ERRORcode", "error");
                    return badInput;
                }
            } else {
                ArrayList operatedColumn = new ArrayList();
                String column1 = joined.types.get(expr.get(0));
                String column2 = joined.types.get(expr.get(2));
                String head1 = expr.get(0);
                String head2 = expr.get(2);
                String resultType = "";
                if (expr.get(1).equals("+")) {
                    if (column1.equals(column2) && column2.equals("string")) {
                        for (int i = 0; i < joined.getNumRows(); i += 1) {
                            if (joined.table.get(head1).get(i).equals("NaN") || joined.table.get(head2).get(i).equals("NaN")) {
                                operatedColumn.add("NaN");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") && joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NOVALUE");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") || joined.table.get(head2).get(i).equals("NOVALUE")) {
                                if (joined.table.get(head1).get(i).equals("NOVALUE")) {
                                    operatedColumn.add(joined.table.get(head2).get(i));
                                } else {
                                    operatedColumn.add(joined.table.get(head1).get(i));
                                }
                            } else {
                                String str1 = joined.table.get(head1).get(i).toString();
                                String str2 = joined.table.get(head2).get(i).toString();
                                str1 = str1.substring(0, str1.length() - 1);
                                str2 = str2.substring(1, str2.length());
                                operatedColumn.add(str1 + str2);
                            }
                        }
                        resultType = "string";
                    } else if (column1.equals(column2) && column2.equals("int")) {
                        for (int i = 0; i < joined.getNumRows(); i += 1) {
                            if (joined.table.get(head1).get(i).equals("NaN") || joined.table.get(head2).get(i).equals("NaN")) {
                                operatedColumn.add("NaN");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") && joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NOVALUE");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") || joined.table.get(head2).get(i).equals("NOVALUE")) {
                                if (joined.table.get(head1).get(i).equals("NOVALUE")) {
                                    operatedColumn.add(joined.table.get(head2).get(i));
                                } else {
                                    operatedColumn.add(joined.table.get(head1).get(i));
                                }
                            } else {
                                operatedColumn.add(((Integer) joined.table.get(head1).get(i)) + ((Integer) joined.table.get(head2).get(i)));
                            }
                        }
                        resultType = "int";
                    } else if (column1.equals(column2) && column2.equals("float")) {
                        for (int i = 0; i < joined.getNumRows(); i += 1) {
                            if (joined.table.get(head1).get(i).equals("NaN") || joined.table.get(head2).get(i).equals("NaN")) {
                                operatedColumn.add("NaN");
                            } else {
                                operatedColumn.add(((Float) joined.table.get(head1).get(i)) + ((Float) joined.table.get(head2).get(i)));
                            }
                        }
                        resultType = "float";
                    } else if (column1.equals("int") && column2.equals("float") ||
                            column1.equals("float") && column2.equals("int")) {
                        for (int i = 0; i < joined.getNumRows(); i += 1) {
                            if (joined.table.get(head1).get(i).equals("NaN") || joined.table.get(head2).get(i).equals("NaN")) {
                                operatedColumn.add("NaN");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") && joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NOVALUE");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") || joined.table.get(head2).get(i).equals("NOVALUE")) {
                                if (joined.table.get(head1).get(i).equals("NOVALUE")) {
                                    operatedColumn.add(joined.table.get(head2).get(i));
                                } else {
                                    operatedColumn.add(joined.table.get(head1).get(i));
                                }
                            } else {
                                Float val = ((Number) joined.table.get(head1).get(i)).floatValue();
                                Float val2 = ((Number) joined.table.get(head2).get(i)).floatValue();
                                operatedColumn.add(val + val2);
                            }
                        }
                        resultType = "float";
                    }

                } else if (expr.get(1).equals("-")) {
                    if (column1.equals(column2) && column2.equals("string")) {

                        Table badInput = new Table();
                        badInput.types.put("ERRORcode", "error");
                        return badInput;
                    } else if (column1.equals(column2) && column2.equals("int")) {
                        for (int i = 0; i < joined.getNumRows(); i += 1) {
                            if (joined.table.get(head1).get(i).equals("NaN") || joined.table.get(head2).get(i).equals("NaN")) {
                                operatedColumn.add("NaN");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") && joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NOVALUE");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") || joined.table.get(head2).get(i).equals("NOVALUE")) {
                                if (joined.table.get(head1).get(i).equals("NOVALUE")) {
                                    operatedColumn.add(joined.table.get(head2).get(i));
                                } else {
                                    operatedColumn.add(joined.table.get(head1).get(i));
                                }
                            } else {
                                operatedColumn.add(((Integer) joined.table.get(head1).get(i)) - ((Integer) joined.table.get(head2).get(i)));
                            }
                        }
                        resultType = "int";
                    } else if (column1.equals(column2) && column2.equals("float")) {
                        for (int i = 0; i < joined.getNumRows(); i += 1) {
                            if (joined.table.get(head1).get(i).equals("NaN") || joined.table.get(head2).get(i).equals("NaN")) {
                                operatedColumn.add("NaN");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") && joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NOVALUE");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") || joined.table.get(head2).get(i).equals("NOVALUE")) {
                                if (joined.table.get(head1).get(i).equals("NOVALUE")) {
                                    operatedColumn.add(joined.table.get(head2).get(i));
                                } else {
                                    operatedColumn.add(joined.table.get(head1).get(i));
                                }
                            } else {
                                operatedColumn.add(((Float) joined.table.get(head1).get(i)) - ((Float) joined.table.get(head2).get(i)));
                            }
                        }
                        resultType = "float";
                    } else if (column1.equals("int") && column2.equals("float") ||
                            column1.equals("float") && column2.equals("int")) {
                        for (int i = 0; i < joined.getNumRows(); i += 1) {
                            if (joined.table.get(head1).get(i).equals("NaN") || joined.table.get(head2).get(i).equals("NaN")) {
                                operatedColumn.add("NaN");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") && joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NOVALUE");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") || joined.table.get(head2).get(i).equals("NOVALUE")) {
                                if (joined.table.get(head1).get(i).equals("NOVALUE")) {
                                    operatedColumn.add(joined.table.get(head2).get(i));
                                } else {
                                    operatedColumn.add(joined.table.get(head1).get(i));
                                }
                            } else {
                                Float val = ((Number) joined.table.get(head1).get(i)).floatValue();
                                Float val2 = ((Number) joined.table.get(head2).get(i)).floatValue();
                                operatedColumn.add(val - val2);
                            }
                        }
                        resultType = "float";
                    }

                } else if (expr.get(1).equals("*")) {
                    if (column1.equals(column2) && column2.equals("string")) {

                        Table badInput = new Table();
                        badInput.types.put("ERRORcode", "error");
                        return badInput;
                    } else if (column1.equals(column2) && column2.equals("int")) {
                        for (int i = 0; i < joined.getNumRows(); i += 1) {
                            if (joined.table.get(head1).get(i).equals("NaN") || joined.table.get(head2).get(i).equals("NaN")) {
                                operatedColumn.add("NaN");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") && joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NOVALUE");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") || joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add((Integer) 0);
                            } else {
                                operatedColumn.add(((Integer) joined.table.get(head1).get(i)) * ((Integer) joined.table.get(head2).get(i)));
                            }
                        }
                        resultType = "int";
                    } else if (column1.equals(column2) && column2.equals("float")) {
                        for (int i = 0; i < joined.getNumRows(); i += 1) {
                            if (joined.table.get(head1).get(i).equals("NaN") || joined.table.get(head2).get(i).equals("NaN")) {
                                operatedColumn.add("NaN");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") && joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NOVALUE");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") || joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add((float) 0);
                            } else {
                                operatedColumn.add(((Float) joined.table.get(head1).get(i)) * ((Float) joined.table.get(head2).get(i)));
                            }
                        }
                        resultType = "float";
                    } else if (column1.equals("int") && column2.equals("float") ||
                            column1.equals("float") && column2.equals("int")) {
                        for (int i = 0; i < joined.getNumRows(); i += 1) {
                            if (joined.table.get(head1).get(i).equals("NaN") || joined.table.get(head2).get(i).equals("NaN")) {
                                operatedColumn.add("NaN");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") && joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NOVALUE");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") || joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add((float) 0);
                            } else {
                                Float val = ((Number) joined.table.get(head1).get(i)).floatValue();
                                Float val2 = ((Number) joined.table.get(head2).get(i)).floatValue();
                                operatedColumn.add(val * val2);
                            }
                        }
                        resultType = "float";
                    }

                } else if (expr.get(1).equals("/")) {
                    if (column1.equals(column2) && column2.equals("string")) {

                        Table badInput = new Table();
                        badInput.types.put("ERRORcode", "error");
                        return badInput;
                    } else if (column1.equals(column2) && column2.equals("int")) {
                        for (int i = 0; i < joined.getNumRows(); i += 1) {
                            if (joined.table.get(head1).get(i).equals("NaN") || joined.table.get(head2).get(i).equals("NaN") ||
                                    ((Integer) joined.table.get(head2).get(i)) == 0 || joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NaN");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") && joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NOVALUE");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE")) {
                                operatedColumn.add(0);
                            } else if (joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NaN");
                            } else {
                                operatedColumn.add(((Integer) joined.table.get(head1).get(i)) / ((Integer) joined.table.get(head2).get(i)));
                            }
                        }
                        resultType = "int";
                    } else if (column1.equals(column2) && column2.equals("float")) {
                        for (int i = 0; i < joined.getNumRows(); i += 1) {
                            if (joined.table.get(head1).get(i).equals("NaN") || joined.table.get(head2).get(i).equals("NaN") ||
                                    ((Float) joined.table.get(head2).get(i)) == 0 || joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NaN");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE") && joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NOVALUE");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE")) {
                                operatedColumn.add((float) 0);
                            } else if (joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NaN");
                            } else {
                                operatedColumn.add(((Float) joined.table.get(head1).get(i)) / ((Float) joined.table.get(head2).get(i)));
                            }
                        }
                        resultType = "float";
                    } else if (column1.equals("int") && column2.equals("float") ||
                            column1.equals("float") && column2.equals("int")) {

                        for (int i = 0; i < joined.getNumRows(); i += 1) {
                            if (joined.table.get(head1).get(i).equals("NOVALUE") && joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NOVALUE");
                            } else if (joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NaN");
                            } else if (joined.table.get(head1).get(i).equals("NaN") || joined.table.get(head2).get(i).equals("NaN") ||
                                    Float.valueOf(String.valueOf(joined.table.get(head2).get(i))) == 0 || joined.table.get(head2).get(i).equals("NOVALUE")) {
                                operatedColumn.add("NaN");
                            } else if (joined.table.get(head1).get(i).equals("NOVALUE")) {
                                operatedColumn.add((float) 0);
                            } else {
                                Float val = ((Number) joined.table.get(head1).get(i)).floatValue();
                                Float val2 = ((Number) joined.table.get(head2).get(i)).floatValue();
                                operatedColumn.add(val / val2);
                            }
                        }
                        resultType = "float";
                    }

                }
                if (select.insertColumn(expr.get(4) + " " + resultType, operatedColumn).equals("ERROR: .*")) {
                    Table badInput = new Table();
                    badInput.types.put("ERRORcode", "error");
                    return badInput;
                }
            }
        }
        return select;
    }

    //combines 2 tables
    public static Table join(Table T1, Table T2) {
        Table T3 = new Table(); //holds the table of the new combined tables
        ArrayList<String> sharedColumns = sharedColumns(T1.headers, T2.headers); //holds the headers that appear in both tables
        ArrayDeque<int[]> rowPairs = new ArrayDeque<>(); //holds pairs of indexes where 2 columns have matching values
        //checks if they share no headers, meaning that they do a cartesian product join
        if (sharedColumns.size() == 0) {
            //the next 2 forloops loop through each table's headers and types and adds them to T3's headers and types
            for (String s : T1.headers) {
                T3.headers.add(s);
                T3.types.put(s, T1.types.get(s));
            }
            for (String s : T2.headers) {
                T3.headers.add(s);
                T3.types.put(s, T2.types.get(s));
            }
            //maps each of T3's header names to an empty arraylist
            for (String header : T3.headers) {
                T3.table.put(header, new ArrayList());
            }
            //adds rows to T3 which are the combined corresponding rows of T1 and T2
            for (int colInd1 = 0; colInd1 < T1.getNumRows(); colInd1 += 1) {
                for (int colInd2 = 0; colInd2 < T2.getNumRows(); colInd2 += 1) {
                    Object[] row = new Object[T1.headers.size() + T2.headers.size()];
                    System.arraycopy(T1.getRowObject(colInd1), 0, row, 0, T1.headers.size());
                    System.arraycopy(T2.getRowObject(colInd2), 0, row, T1.headers.size(), T2.headers.size());
                    T3.addRow(new ArrayList(Arrays.asList(row)));
                }
            }
            return T3;
        }

        String matchedColumn = sharedColumns.get(0);
        //might be some problems here with addAll as i'm unsure if it adds the arraydeque from initialrowpairs or the values inside the arraydeque
        rowPairs.addAll(initialRowsPairs(T1.table.get(matchedColumn), T2.table.get(matchedColumn)));
        if (sharedColumns.size() > 1) {
            rowPairs = finalRowPairs(sharedColumns, rowPairs, T1, T2);
        }


        for (String s : sharedColumns) {
            T3.headers.add(s);
            T3.types.put(s, T1.types.get(s));
            ArrayList sColumn = joinedColumn(T1.table.get(s), rowPairs, 0);
            T3.table.put(s, sColumn);
        }

        for (String s : T1.headers) {
            if (!sharedColumns.contains(s)) {
                T3.headers.add(s);
                T3.types.put(s, T1.types.get(s));
                ArrayList sColumn = joinedColumn(T1.table.get(s), rowPairs, 0);
                T3.table.put(s, sColumn);
            }
        }

        for (String s : T2.headers) {
            if (!sharedColumns.contains(s)) {
                T3.headers.add(s);
                T3.types.put(s, T2.types.get(s));
                ArrayList sColumn = joinedColumn(T2.table.get(s), rowPairs, 1);
                T3.table.put(s, sColumn);
            }
        }
        return T3;
    }

    //gets a list of headers that show up in both tables' headers arraylist
    private static ArrayList<String> sharedColumns(ArrayList<String> headers1, ArrayList<String> headers2) {
        ArrayList<String> matches = new ArrayList<>();
        for (String s : headers1) {
            if (headers2.contains(s)) {
                matches.add(s);
            }
        }
        return matches;
    }

    //gets an ArrayDeque that holds the pairs of indexes where two columns share the same value
    private static ArrayDeque<int[]> initialRowsPairs(ArrayList column1, ArrayList column2) {
        ArrayDeque<int[]> rowPairs = new ArrayDeque<>();
        for (int i = 0; i < column1.size(); i += 1) {
            if (column2.contains(column1.get(i))) {
                for (int index = 0; index < column2.size(); index += 1) {
                    if (column1.get(i).equals(column2.get(index))) {
                        rowPairs.addLast(new int[]{i, index});
                    }
                }
            }
        }
        return rowPairs;
    }

    //
    private static ArrayDeque<int[]> finalRowPairs(List<String> sharedColumns, ArrayDeque<int[]> rowPairs, Table T1, Table T2) {
        ArrayDeque<int[]> validRows = new ArrayDeque<>();
        for (int[] pair : rowPairs) {
            if (filterRow(T1, T2, sharedColumns, pair)) {
                validRows.addLast(pair);
            }
        }
        return validRows;
    }

    //sees if the values at a matching index in a shared header dont match up
    //match is a 2 length array holding the column index where there is a match in values
    private static boolean filterRow(Table T1, Table T2, List<String> sharedColumns, int[] match) {
        for (String s : sharedColumns) { //loops through each header that appears in both tables
            //sees if the values at a matching index in a shared header don't match up
            if (!T1.table.get(s).get(match[0]).equals(T2.table.get(s).get(match[1]))) {
                return false;
            }
        }
        return true;
    }

    private static ArrayList joinedColumn(ArrayList fullColumn, ArrayDeque<int[]> rowPairs, int rowIndex) {
        ArrayList newColumn = new ArrayList();
        for (int[] pair : rowPairs) {
            newColumn.add(fullColumn.get(pair[rowIndex]));
        }
        return newColumn;
    }

    //conditionals example: [["x", ">", "'hellothere'"], ["ayo", "<=", "hhh"]]
    public static Table evalCond2(Table oldCond, ArrayList<ArrayList<String>> conditionals) {
        Table badInput = new Table();
        badInput.types.put("ERRORcode", "error");
        Table output = oldCond.makeClone();
        for (ArrayList<String> conditionalSet : conditionals) {
            String varLeft = conditionalSet.get(0);
            String varRight = conditionalSet.get(2);
            String leftType = determineType(varLeft);
            String rightType = determineType(varRight);
            String inequality = conditionalSet.get(1);
            if (varLeft.matches("[-+]?\\d*\\.?\\d+") && varRight.matches("[-+]?\\d*\\.?\\d+") ||
                    !(output.headers.contains(varLeft)) && !(output.headers.contains(varRight))) {

                return badInput;
            }

            if (leftType.equals("columnName") && !rightType.equals("columnName")) {

                for (int i = 0; i < output.table.get(varLeft).size(); i++) {

                    if (whichCompare(output.types.get(varLeft), rightType).equals("ERROR: .*")) {

                        return badInput;
                    }
                    String currentLeft = output.table.get(varLeft).get(i).toString();
                    if (compareRightTypes(currentLeft, output.types.get(varLeft), inequality, varRight, rightType) == false) {
                        output.removeRow(i);
                        i--;
                    }
                }
            } else if (!leftType.equals("columnName") && rightType.equals("columnName")) {
                for (int i = 0; i < output.table.get(varRight).size(); i++) {
                    if (whichCompare(output.types.get(varRight), rightType).equals("ERROR: .*")) {

                        return badInput;
                    }
                    String currentRight = output.table.get(varRight).get(i).toString();
                    if (compareRightTypes(varLeft, leftType, inequality, currentRight, output.types.get(varRight)) == false) {
                        output.removeRow(i);
                        i--;
                    }
                }
            } else if (leftType.equals("columnName") && rightType.equals("columnName")) {

                for (int i = 0; i < output.table.get(varLeft).size(); i++) {
                    if (whichCompare(output.types.get(varLeft), output.types.get(varRight)).equals("ERROR: .*")) {

                        return badInput;
                    }
                    String currentLeft = output.table.get(varLeft).get(i).toString();
                    String currentRight = output.table.get(varRight).get(i).toString();
                    if (compareRightTypes(currentLeft, output.types.get(varLeft), inequality, currentRight, output.types.get(varRight)) == false) {
                        output.removeRow(i);
                        i--;
                    }
                }
            } else {

                return badInput;
            }
        }

        return output;
    }

    private static String whichCompare(String leftType, String rightType) {
        if (leftType.equals("string") && rightType.equals("string")) {
            return "string";
        } else if (leftType.equals("int") && rightType.equals("int")) {
            return "int";
        } else if (leftType.equals("float") && rightType.equals("float")) {
            return "float";
        } else if (leftType.equals("float") && rightType.equals("int")) {
            return "float";
        } else if (leftType.equals("int") && rightType.equals("float")) {
            return "float";
        } else {
            return "ERROR: .*";
        }
    }

    //will perform the inequalities of different types depending on if the left/right is a string, int, or float
    private static boolean compareRightTypes(String leftVar, String leftType, String inequality, String rightVar, String rightType) {

        if (whichCompare(leftType, rightType).equals("string")) {

            return compareString(leftVar, inequality, rightVar);
        } else if (whichCompare(leftType, rightType).equals("float")) {

            return compareFloat(leftVar, inequality, rightVar);
        } else if (whichCompare(leftType, rightType).equals("int")) {
            return compareInteger(leftVar, inequality, rightVar);
        }
        return false;
    }

    //occurs when one variable is NaN, this then compares the inequalities in terms of NaN
    private static boolean compareNaN(String leftStr, String inequality, String rightStr) {
        if (leftStr.equals("NaN") && rightStr.equals("NaN")) {
            if (inequality.equals("==")) {
                return true;
            }
            return false;
        } else if (leftStr.equals("NaN") && !rightStr.equals("NaN")) {
            if (inequality.equals(">") || inequality.equals(">=")) {
                return true;
            }
            return false;
        } else if (!leftStr.equals("NaN") && rightStr.equals("NaN")) {
            if (inequality.equals("<") || inequality.equals("<=")) {
                return true;
            }
            return false;
        } else {
            return false;
        }
    }

    private static boolean compareInteger(String leftStr, String inequality, String rightStr) {
        if (leftStr.equals("NOVALUE") || rightStr.equals("NOVALUE")) {
            return false;
        } else if (leftStr.equals("NaN") || rightStr.equals("NaN")) {
            return compareNaN(leftStr, inequality, rightStr);
        } else if (inequality.equals(">")) {
            return Integer.parseInt(leftStr) > Integer.parseInt(rightStr);
        } else if (inequality.equals("<")) {
            return Integer.parseInt(leftStr) < Integer.parseInt(rightStr);
        } else if (inequality.equals(">=")) {
            return Integer.parseInt(leftStr) >= Integer.parseInt(rightStr);
        } else if (inequality.equals("<=")) {
            return Integer.parseInt(leftStr) <= Integer.parseInt(rightStr);
        } else if (inequality.equals("==")) {
            return Integer.parseInt(leftStr) == Integer.parseInt(rightStr);
        } else if (inequality.equals("!=")) {
            return Integer.parseInt(leftStr) != Integer.parseInt(rightStr);
        }
        return false;
    }

    private static boolean compareFloat(String leftStr, String inequality, String rightStr) {
        if (leftStr.equals("NOVALUE") || rightStr.equals("NOVALUE")) {
            return false;
        } else if (leftStr.equals("NaN") || rightStr.equals("NaN")) {
            return compareNaN(leftStr, inequality, rightStr);
        } else if (inequality.equals(">")) {
            return Float.parseFloat(leftStr) > Float.parseFloat(rightStr);
        } else if (inequality.equals("<")) {
            return Float.parseFloat(leftStr) < Float.parseFloat(rightStr);
        } else if (inequality.equals(">=")) {
            return Float.parseFloat(leftStr) >= Float.parseFloat(rightStr);
        } else if (inequality.equals("<=")) {
            return Float.parseFloat(leftStr) <= Float.parseFloat(rightStr);
        } else if (inequality.equals("==")) {
            return Float.parseFloat(leftStr) == Float.parseFloat(rightStr);
        } else if (inequality.equals("!=")) {
            return Float.parseFloat(leftStr) != Float.parseFloat(rightStr);
        }
        return false;
    }

    //gives the boolean value of >, <, >=, <=, ==, !=, for strings
    private static boolean compareString(String leftStr, String inequality, String rightStr) {

        if (leftStr.equals("NOVALUE") || rightStr.equals("NOVALUE")) {
            return false;
        } else if (leftStr.equals("NaN") || rightStr.equals("NaN")) {
            return compareNaN(leftStr, inequality, rightStr);
        } else if (inequality.equals(">")) {
            return leftStr.compareTo(rightStr) > 0;
        } else if (inequality.equals("<")) {
            return leftStr.compareTo(rightStr) < 0;
        } else if (inequality.equals(">=")) {
            return leftStr.compareTo(rightStr) >= 0;
        } else if (inequality.equals("<=")) {
            return leftStr.compareTo(rightStr) <= 0;
        } else if (inequality.equals("==")) {
            return leftStr.compareTo(rightStr) == 0;
        } else if (inequality.equals("!=")) {
            return leftStr.compareTo(rightStr) != 0;
        }
        return false;
    }

    //outputs whether the variable is a columnName, Integer, Float, or string
    private static String determineType(String var) {
        if (var.contains("\'")) {
            return "string";
        } else if (var.matches("[-+]?\\d*\\.?\\d+")) {
            if (var.contains(".")) {
                return "float";
            }
            return "int";
        }
        return "columnName";
    }

    /*protected void floatAlign() {
        if (types.keySet().contains("float")) {
            for (String s : table.keySet()) {
                if (types.get(s).equals("float")) {
                    table.put(s, floatColumnAlign(table.get(s)));
                }
            }
        }
    }

    protected ArrayList floatColumnAlign(ArrayList<Float> column) {
        ArrayList aligned = new ArrayList();
        for (Float value : column) {
            aligned.add(Float.parseFloat(String.format("%.3f", value)));
        }
        return aligned;
    }*/
}
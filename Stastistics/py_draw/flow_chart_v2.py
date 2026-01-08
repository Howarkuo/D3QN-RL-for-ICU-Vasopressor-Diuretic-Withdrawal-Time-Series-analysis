import graphviz

def create_clinical_table():
    # Initialize the Digraph
    dot = graphviz.Digraph(comment='Clinical Characteristics Table')
    
    # Define the data rows
    data = [
        ["n", "", "", "2533", "804", "1729", ""],
        ["anchor_age, median [Q1,Q3]", "", "0", "69.0 [59.0,78.0]", "72.0 [62.8,80.0]", "67.0 [57.0,76.0]", "<0.001"],
        ["icu_los_days, median [Q1,Q3]", "", "0", "4.8 [2.7,8.9]", "4.5 [2.3,8.7]", "5.0 [2.9,8.9]", "0.001"],
        ["resp_rate, median [Q1,Q3]", "", "4", "19.8 [17.5,22.7]", "20.6 [17.9,23.6]", "19.5 [17.3,22.2]", "<0.001"],
        ["spo2, median [Q1,Q3]", "", "4", "97.1 [95.6,98.5]", "96.9 [95.3,98.4]", "97.2 [95.7,98.5]", "0.021"],
        ["po2, median [Q1,Q3]", "", "260", "112.5 [72.5,170.0]", "105.8 [74.3,155.0]", "118.2 [72.0,178.6]", "0.020"],
        ["fio2, median [Q1,Q3]", "", "1766", "60.0 [50.0,87.0]", "65.0 [50.0,99.0]", "60.0 [50.0,80.0]", "0.008"],
        ["sbp, median [Q1,Q3]", "", "11", "105.3 [98.4,112.9]", "103.3 [96.7,111.2]", "106.0 [99.4,113.6]", "<0.001"],
        ["heart_rate, median [Q1,Q3]", "", "3", "87.1 [76.7,99.3]", "89.2 [78.1,102.3]", "86.0 [76.4,97.8]", "<0.001"],
        ["cardiac_output, median [Q1,Q3]", "", "2004", "4.6 [3.7,5.4]", "4.5 [3.6,5.5]", "4.6 [3.7,5.4]", "0.287"],
        ["cardiac_index, median [Q1,Q3]", "", "2026", "2.3 [2.0,2.7]", "2.3 [1.8,2.6]", "2.3 [2.0,2.7]", "0.111"],
        ["pcwp, median [Q1,Q3]", "", "2277", "22.0 [17.2,26.5]", "22.0 [17.1,26.9]", "22.0 [17.3,26.0]", "0.889"],
        ["lactate, median [Q1,Q3]", "", "331", "3.4 [2.1,6.5]", "4.6 [2.4,8.6]", "3.2 [1.9,5.7]", "<0.001"],
        ["ph, median [Q1,Q3]", "", "262", "7.4 [7.3,7.4]", "7.3 [7.3,7.4]", "7.4 [7.3,7.4]", "<0.001"],
        ["bicarbonate, median [Q1,Q3]", "", "2477", "21.0 [16.8,23.0]", "15.2 [14.5,16.4]", "21.8 [19.0,24.2]", "<0.001"],
        ["creatinine, median [Q1,Q3]", "", "8", "1.7 [1.2,2.6]", "2.1 [1.4,3.1]", "1.6 [1.1,2.3]", "<0.001"],
        ["urine_output_24h, median [Q1,Q3]", "", "80", "1285.0 [605.0,2423.7]", "742.5 [258.2,1599.3]", "1575.0 [881.5,2773.5]", "<0.001"],
        ["max_ned_24h, median [Q1,Q3]", "", "0", "0.2 [0.0,1.2]", "0.5 [0.0,6.2]", "0.2 [0.0,0.8]", "<0.001"],
        
        # Categorical Variables
        ["gender, n (%)", "Female", "", "972 (38.4)", "349 (43.4)", "623 (36.0)", "<0.001"],
        ["", "Male", "", "1561 (61.6)", "455 (56.6)", "1106 (64.0)", ""],
        
        ["Vasopressor Use (Any), n (%)", "No / Missing", "", "840 (33.2)", "209 (26.0)", "631 (36.5)", "<0.001"],
        ["", "Yes", "", "1693 (66.8)", "595 (74.0)", "1098 (63.5)", ""],
        
        ["use_norepi, n (%)", "No / Missing", "", "1269 (50.1)", "312 (38.8)", "957 (55.3)", "<0.001"],
        ["", "Yes", "", "1264 (49.9)", "492 (61.2)", "772 (44.7)", ""],
        
        ["use_epi, n (%)", "No / Missing", "", "2020 (79.7)", "641 (79.7)", "1379 (79.8)", "1.000"],
        ["", "Yes", "", "513 (20.3)", "163 (20.3)", "350 (20.2)", ""],
        
        ["use_dopa, n (%)", "No / Missing", "", "2144 (84.6)", "660 (82.1)", "1484 (85.8)", "0.018"],
        ["", "Yes", "", "389 (15.4)", "144 (17.9)", "245 (14.2)", ""],
        
        ["use_phenyl, n (%)", "No / Missing", "", "2031 (80.2)", "631 (78.5)", "1400 (81.0)", "0.159"],
        ["", "Yes", "", "502 (19.8)", "173 (21.5)", "329 (19.0)", ""],
        
        ["use_vaso, n (%)", "No / Missing", "", "2033 (80.3)", "588 (73.1)", "1445 (83.6)", "<0.001"],
        ["", "Yes", "", "500 (19.7)", "216 (26.9)", "284 (16.4)", ""]
    ]

    # Begin constructing the HTML Label
    # FIX: The empty column header is now just <TD></TD> instead of <TD><B></B></TD>
    html_label = '''<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
    <TR>
        <TD COLSPAN="7" BGCOLOR="LIGHTGREY"><B>TABLE 1: CLINICAL CHARACTERISTICS (Values &amp; Counts)</B></TD>
    </TR>
    <TR>
        <TD><B>Feature</B></TD>
        <TD></TD>
        <TD><B>Missing</B></TD>
        <TD><B>Overall</B></TD>
        <TD><B>Non-survivor</B></TD>
        <TD><B>Survivor</B></TD>
        <TD><B>P-Value</B></TD>
    </TR>'''

    # Loop through data to create rows
    for row in data:
        html_label += "<TR>"
        for i, cell in enumerate(row):
            # Escape the < symbol
            formatted_cell = cell.replace("<", "&lt;")
            
            # Align text
            align = "LEFT" if i == 0 else "CENTER"
            
            # Add cell
            html_label += f'<TD ALIGN="{align}">{formatted_cell}</TD>'
        html_label += "</TR>"

    html_label += "</TABLE>>"

    dot.node('tab1', label=html_label, shape='plaintext', fontname='Helvetica')

    return dot

if __name__ == "__main__":
    try:
        dot = create_clinical_table()
        dot.render('table1', format='pdf', view=True)
        print("Table generated successfully as table1.pdf")
    except Exception as e:
        print(f"Error: {e}")
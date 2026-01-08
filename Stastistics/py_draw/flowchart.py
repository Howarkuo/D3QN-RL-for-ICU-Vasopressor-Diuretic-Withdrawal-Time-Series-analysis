# import os

# # Point strictly to the 'bin' folder
# os.environ["PATH"] += os.pathsep + r'C:\Program Files (x86)\Graphviz\bin'

# import graphviz  # Now import graphviz

# # ... The rest of your code ...

# import graphviz

# def create_prisma_mimic_diagram():
#     # Initialize the graph
#     # 'splines=ortho' ensures right-angled arrows
#     # 'rankdir=TB' sets direction Top-to-Bottom
#     dot = graphviz.Digraph(comment='PRISMA Flow Diagram', format='png')
#     dot.attr(rankdir='TB', splines='ortho', nodesep='1.0', ranksep='0.8')
    
#     # --- Node Styles ---
#     # Box style for main steps
#     dot.attr('node', shape='box', style='filled', fillcolor='white', 
#              fontname='Arial', fontsize='15', width='3.5', height='1.2', penwidth='1.2')
    
#     # --- 1. IDENTIFICATION PHASE ---
#     # Top box: Initial Database Population
#     dot.node('ID_DB', '''Identified from Databases (MIMIC-IV v2.2)
# (n = 299,712)''', fillcolor='#E3F2FD') # Light Blue
    
#     # --- 2. SCREENING PHASE ---
#     # Screened Records
#     dot.node('Screened', '''Cohort Screened
# (n = 299,712)''')

#     # Excluded Records (Side Box)
#     dot.node('Excluded_Screen', '''Cohort Excluded
# (n = 257,449)
# • Age ≤ 18
# • ICU Stay ≤ 24 hours''', 
#              shape='box', style='dashed', fillcolor='#FFEBEE', width='3.0', align='left')

#     # --- 3. ELIGIBILITY PHASE (Reports Assessed) ---
#     # Reports sought for retrieval
#     dot.node('Sought', '''Cohort Sought for Retrieval
# (n = 42,263)''')

#     # Reports not retrieved (Side Box - usually 0 for databases)
#     dot.node('Not_Retrieved', '''Cohort not retrieved
# (n = 0)''', 
#              shape='box', style='dashed', fillcolor='#FFEBEE', width='3.0')

#     # Reports assessed for eligibility
#     dot.node('Assessed', '''Cohort Assessed for Cardiogenic-Shock ICD Diagnosis Code
# ('78551', '99801', 'R570', 'T8111')
# (n = 42,263)''')

#     # Reports Excluded (Side Box)
#     dot.node('Excluded_Full', '''Cohort Excluded
# (n = 40,287)
# • No Cardiogenic Shock Diagnosis''', 
#              shape='box', style='dashed', fillcolor='#FFEBEE', width='3.0', align='left')

#     # --- 4. INCLUDED PHASE ---
#     # Final Result
#     dot.node('Included', '''New Studies Included
# (Final Cohort)
# (n = 1,976)''', 
#              fillcolor='#E8F5E9', penwidth='2.0') # Light Green

#     # --- INVISIBLE NODES (For Layout Alignment) ---
#     # These invisible nodes force the side boxes to align horizontally with the main boxes
#     dot.attr('node', style='invis', width='0', label='')
#     dot.node('inv1')
#     dot.node('inv2')
#     dot.node('inv3')

#     # --- EDGES (Connections) ---
    
#     # 1. Main Vertical Flow
#     dot.edge('ID_DB', 'Screened')
#     dot.edge('Screened', 'Sought')
#     dot.edge('Sought', 'Assessed')
#     dot.edge('Assessed', 'Included')

#     # 2. Exclusion Flows (Side Arrows)
#     # We use 'rank=same' to force horizontal alignment
    
#     # Alignment 1: Screening -> Excluded
#     with dot.subgraph() as s:
#         s.attr(rank='same')
#         s.node('Screened')
#         s.node('Excluded_Screen')
#     dot.edge('Screened', 'Excluded_Screen')

#     # Alignment 2: Sought -> Not Retrieved
#     with dot.subgraph() as s:
#         s.attr(rank='same')
#         s.node('Sought')
#         s.node('Not_Retrieved')
#     dot.edge('Sought', 'Not_Retrieved')

#     # Alignment 3: Assessed -> Excluded
#     with dot.subgraph() as s:
#         s.attr(rank='same')
#         s.node('Assessed')
#         s.node('Excluded_Full')
#     dot.edge('Assessed', 'Excluded_Full')

#     # Render and View
#     output_path = dot.render('mimic_prisma_flowchart', view=True)
#     print(f"Flowchart generated successfully at: {output_path}")

# if __name__ == "__main__":
#     create_prisma_mimic_diagram()

import graphviz
import os

# Uncomment and adjust if you still need to manually set the path
os.environ["PATH"] += os.pathsep + r'C:\Program Files (x86)\Graphviz\bin'

def create_detailed_prisma_diagram():
    dot = graphviz.Digraph(comment='PRISMA Flow Diagram', format='png')
    dot.attr(rankdir='TB', splines='ortho', nodesep='1.0', ranksep='0.8')
    
    # Base Node Style (plain shape allows the HTML table to define the border)
    dot.attr('node', shape='plain', fontname='Arial', fontsize='15')

    # --- 1. IDENTIFICATION PHASE ---
    # Note the triple quotes (''') surrounding the whole label
    label_start = '''<
    <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4" BGCOLOR="#E3F2FD">
      <TR><TD COLSPAN="2"><B>Identified from Databases (MIMIC-IV)</B></TD></TR>
      <TR><TD ALIGN="LEFT">Patients:</TD><TD ALIGN="RIGHT">299,712</TD></TR>
      <TR><TD ALIGN="LEFT">Admissions (hadm_id):</TD><TD ALIGN="RIGHT">431,231</TD></TR>
      <TR><TD ALIGN="LEFT">ICU Stays (stay_id):</TD><TD ALIGN="RIGHT">73,181</TD></TR>
    </TABLE>>'''
    dot.node('ID_DB', label_start)
    
    # --- 2. SCREENING PHASE ---
    label_screened = '''<
    <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4" BGCOLOR="white">
      <TR><TD><B>Records Screened</B></TD></TR>
      <TR><TD>Patients: 299,712</TD></TR>
    </TABLE>>'''
    dot.node('Screened', label_screened)

    # Exclusion Box
    label_exclude_1 = '''<
    <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4" BGCOLOR="#FFEBEE" STYLE="dashed">
      <TR><TD COLSPAN="2"><B>Records Excluded</B></TD></TR>
      <TR><TD ALIGN="LEFT">Reason:</TD><TD ALIGN="LEFT">• Age &le; 18<BR/>• ICU Stay &le; 24h</TD></TR>
    </TABLE>>'''
    dot.node('Excluded_Screen', label_exclude_1)

    # --- 3. ELIGIBILITY PHASE ---
    label_sought = '''<
    <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4" BGCOLOR="white">
      <TR><TD><B>Reports Sought</B></TD></TR>
      <TR><TD>Patients: 42,263</TD></TR>
    </TABLE>>'''
    dot.node('Sought', label_sought)
    
    label_not_retrieved = '''<
    <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4" BGCOLOR="#FFEBEE" STYLE="dashed">
      <TR><TD><B>Not retrieved (n=0)</B></TD></TR>
    </TABLE>>'''
    dot.node('Not_Retrieved', label_not_retrieved)

    # Detailed Middle Step (Stable Cohort)
    label_assessed = '''<
    <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4" BGCOLOR="white">
      <TR><TD COLSPAN="2"><B>Assessed for Eligibility</B><BR/>(Stable Adult Cohort)</TD></TR>
      <TR><TD ALIGN="LEFT">Patients:</TD><TD ALIGN="RIGHT">42,263</TD></TR>
      <TR><TD ALIGN="LEFT">Stays (stay_id):</TD><TD ALIGN="RIGHT">57,732</TD></TR>
    </TABLE>>'''
    dot.node('Assessed', label_assessed)

    label_exclude_2 = '''<
    <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4" BGCOLOR="#FFEBEE" STYLE="dashed">
      <TR><TD COLSPAN="2"><B>Reports Excluded</B></TD></TR>
      <TR><TD ALIGN="LEFT">Reason:</TD><TD ALIGN="LEFT">• No Cardiogenic Shock Diagnosis<BR/>• Inconsistent Timeline</TD></TR>
    </TABLE>>'''
    dot.node('Excluded_Full', label_exclude_2)

    # --- 4. INCLUDED PHASE ---
    label_included = '''<
    <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4" BGCOLOR="#E8F5E9">
      <TR><TD COLSPAN="2"><B>New Studies Included</B><BR/>(Final Cohort)</TD></TR>
      <TR><TD ALIGN="LEFT">Subjects:</TD><TD ALIGN="RIGHT">1,976</TD></TR>
      <TR><TD ALIGN="LEFT">ICU Stays (stay_id):</TD><TD ALIGN="RIGHT">2,531</TD></TR>
    </TABLE>>'''
    dot.node('Included', label_included)

    # --- EDGES & ALIGNMENT ---
    dot.edge('ID_DB', 'Screened')
    dot.edge('Screened', 'Sought')
    dot.edge('Sought', 'Assessed')
    dot.edge('Assessed', 'Included')

    # Force Alignment
    with dot.subgraph() as s:
        s.attr(rank='same')
        s.node('Screened')
        s.node('Excluded_Screen')
    dot.edge('Screened', 'Excluded_Screen')

    with dot.subgraph() as s:
        s.attr(rank='same')
        s.node('Sought')
        s.node('Not_Retrieved')
    dot.edge('Sought', 'Not_Retrieved')

    with dot.subgraph() as s:
        s.attr(rank='same')
        s.node('Assessed')
        s.node('Excluded_Full')
    dot.edge('Assessed', 'Excluded_Full')

    # Render
    dot.render('mimic_prisma_detailed', view=True)
    print("Diagram generated: mimic_prisma_detailed.png")

if __name__ == "__main__":
    create_detailed_prisma_diagram()
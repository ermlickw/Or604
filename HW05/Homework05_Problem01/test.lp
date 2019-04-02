\ Model Cow model
\ LP format - for model browsing. Use MPS format to capture full model detail.
Minimize
  125 CowsCalved(1) + 0.334 Excess(1) + 1.67 Shortage(1) + 135 CowsCalved(2)
   + 0.344 Excess(2) + 1.72 Shortage(2) + 125 CowsCalved(3)
   + 0.37 Excess(3) + 1.85 Shortage(3) + 115 CowsCalved(4)
   + 0.366 Excess(4) + 1.83 Shortage(4) + 105 CowsCalved(5)
   + 0.352 Excess(5) + 1.76 Shortage(5) + 95 CowsCalved(6)
   + 0.328 Excess(6) + 1.64 Shortage(6) + 85 CowsCalved(7)
   + 0.326 Excess(7) + 1.63 Shortage(7) + 95 CowsCalved(8)
   + 0.318 Excess(8) + 1.59 Shortage(8) + 105 CowsCalved(9)
   + 0.322 Excess(9) + 1.61 Shortage(9) + 110 CowsCalved(10)
   + 0.324 Excess(10) + 1.62 Shortage(10) + 115 CowsCalved(11)
   + 0.33 Excess(11) + 1.65 Shortage(11) + 120 CowsCalved(12)
   + 0.35 Excess(12) + 1.75 Shortage(12)
Subject To
 MonthlyDemand_1: 62 CowsCalved(1) - Excess(1) + Shortage(1)
   + 42 CowsCalved(2) + 31 CowsCalved(3) + 7.5 CowsCalved(4)
   + 15.5 CowsCalved(5) + 30 CowsCalved(6) + 46.5 CowsCalved(7)
   + 62 CowsCalved(8) + 75 CowsCalved(9) + 85.25 CowsCalved(10)
   + 90 CowsCalved(11) + 77.5 CowsCalved(12) = 25000
 MonthlyDemand_2: 77.5 CowsCalved(1) + 56 CowsCalved(2) - Excess(2)
   + Shortage(2) + 46.5 CowsCalved(3) + 30 CowsCalved(4)
   + 7.75 CowsCalved(5) + 15 CowsCalved(6) + 31 CowsCalved(7)
   + 46.5 CowsCalved(8) + 60 CowsCalved(9) + 77.5 CowsCalved(10)
   + 82.5 CowsCalved(11) + 93 CowsCalved(12) = 27500
 MonthlyDemand_3: 93 CowsCalved(1) + 70 CowsCalved(2) + 62 CowsCalved(3)
   - Excess(3) + Shortage(3) + 45 CowsCalved(4) + 31 CowsCalved(5)
   + 7.5 CowsCalved(6) + 15.5 CowsCalved(7) + 31 CowsCalved(8)
   + 45 CowsCalved(9) + 62 CowsCalved(10) + 75 CowsCalved(11)
   + 85.25 CowsCalved(12) = 30000
 MonthlyDemand_4: 85.25 CowsCalved(1) + 84 CowsCalved(2)
   + 77.5 CowsCalved(3) + 60 CowsCalved(4) - Excess(4) + Shortage(4)
   + 46.5 CowsCalved(5) + 30 CowsCalved(6) + 7.75 CowsCalved(7)
   + 15.5 CowsCalved(8) + 30 CowsCalved(9) + 46.5 CowsCalved(10)
   + 60 CowsCalved(11) + 77.5 CowsCalved(12) = 27000
 MonthlyDemand_5: 77.5 CowsCalved(1) + 77 CowsCalved(2) + 93 CowsCalved(3)
   + 75 CowsCalved(4) + 62 CowsCalved(5) - Excess(5) + Shortage(5)
   + 45 CowsCalved(6) + 31 CowsCalved(7) + 7.75 CowsCalved(8)
   + 15 CowsCalved(9) + 31 CowsCalved(10) + 45 CowsCalved(11)
   + 62 CowsCalved(12) = 26000
 MonthlyDemand_6: 62 CowsCalved(1) + 70 CowsCalved(2) + 85.25 CowsCalved(3)
   + 90 CowsCalved(4) + 77.5 CowsCalved(5) + 60 CowsCalved(6) - Excess(6)
   + Shortage(6) + 46.5 CowsCalved(7) + 31 CowsCalved(8)
   + 7.5 CowsCalved(9) + 15.5 CowsCalved(10) + 30 CowsCalved(11)
   + 46.5 CowsCalved(12) = 23500
 MonthlyDemand_7: 46.5 CowsCalved(1) + 56 CowsCalved(2)
   + 77.5 CowsCalved(3) + 82.5 CowsCalved(4) + 93 CowsCalved(5)
   + 75 CowsCalved(6) + 62 CowsCalved(7) - Excess(7) + Shortage(7)
   + 46.5 CowsCalved(8) + 30 CowsCalved(9) + 7.75 CowsCalved(10)
   + 15 CowsCalved(11) + 31 CowsCalved(12) = 22000
 MonthlyDemand_8: 31 CowsCalved(1) + 42 CowsCalved(2) + 62 CowsCalved(3)
   + 75 CowsCalved(4) + 85.25 CowsCalved(5) + 90 CowsCalved(6)
   + 77.5 CowsCalved(7) + 62 CowsCalved(8) - Excess(8) + Shortage(8)
   + 45 CowsCalved(9) + 31 CowsCalved(10) + 7.5 CowsCalved(11)
   + 15.5 CowsCalved(12) = 21500
 MonthlyDemand_9: 15.5 CowsCalved(1) + 28 CowsCalved(2)
   + 46.5 CowsCalved(3) + 60 CowsCalved(4) + 77.5 CowsCalved(5)
   + 82.5 CowsCalved(6) + 93 CowsCalved(7) + 77.5 CowsCalved(8)
   + 60 CowsCalved(9) - Excess(9) + Shortage(9) + 46.5 CowsCalved(10)
   + 30 CowsCalved(11) + 7.75 CowsCalved(12) = 22750
 MonthlyDemand_10: 7.75 CowsCalved(1) + 14 CowsCalved(2) + 31 CowsCalved(3)
   + 45 CowsCalved(4) + 62 CowsCalved(5) + 75 CowsCalved(6)
   + 85.25 CowsCalved(7) + 93 CowsCalved(8) + 75 CowsCalved(9)
   + 62 CowsCalved(10) - Excess(10) + Shortage(10) + 45 CowsCalved(11)
   + 31 CowsCalved(12) = 23000
 MonthlyDemand_11: 31 CowsCalved(1) + 7 CowsCalved(2) + 15.5 CowsCalved(3)
   + 30 CowsCalved(4) + 46.5 CowsCalved(5) + 60 CowsCalved(6)
   + 77.5 CowsCalved(7) + 85.25 CowsCalved(8) + 90 CowsCalved(9)
   + 77.5 CowsCalved(10) + 60 CowsCalved(11) - Excess(11) + Shortage(11)
   + 46.5 CowsCalved(12) = 25000
 MonthlyDemand_12: 46.5 CowsCalved(1) + 28 CowsCalved(2)
   + 7.75 CowsCalved(3) + 15 CowsCalved(4) + 31 CowsCalved(5)
   + 45 CowsCalved(6) + 62 CowsCalved(7) + 77.5 CowsCalved(8)
   + 82.5 CowsCalved(9) + 93 CowsCalved(10) + 75 CowsCalved(11)
   + 62 CowsCalved(12) - Excess(12) + Shortage(12) = 27000
Bounds
Generals
 CowsCalved(1) CowsCalved(2) CowsCalved(3) CowsCalved(4) CowsCalved(5)
 CowsCalved(6) CowsCalved(7) CowsCalved(8) CowsCalved(9) CowsCalved(10)
 CowsCalved(11) CowsCalved(12)
End

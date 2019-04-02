\ Model model name
\ LP format - for model browsing. Use MPS format to capture full model detail.
Maximize
  1200 Slot(NB,1) + 3100 Slot(DR,1) + 3000 Slot(DOLR,1) + 550 Slot(GP,1)
   + 2000 Slot(HDD,1) + 2000 Slot(ML,1) + 2500 Slot(BJK,1)
   + 1200 Slot(NB,2) + 3100 Slot(DR,2) + 3000 Slot(DOLR,2) + 550 Slot(GP,2)
   + 2000 Slot(HDD,2) + 2000 Slot(ML,2) + 2500 Slot(BJK,2)
   + 1200 Slot(NB,3) + 3100 Slot(DR,3) + 3000 Slot(DOLR,3) + 550 Slot(GP,3)
   + 2000 Slot(HDD,3) + 2000 Slot(ML,3) + 2500 Slot(BJK,3)
   + 1200 Slot(NB,4) + 3100 Slot(DR,4) + 3000 Slot(DOLR,4) + 550 Slot(GP,4)
   + 2000 Slot(HDD,4) + 2000 Slot(ML,4) + 2500 Slot(BJK,4)
Subject To
 floorspace_1: 2 Slot(NB,1) + 2.5 Slot(DR,1) + 6 Slot(DOLR,1)
   + 2.75 Slot(GP,1) + 3.5 Slot(HDD,1) + 4 Slot(ML,1) + 2.75 Slot(BJK,1)
   <= 750
 floorspace_2: 2 Slot(NB,2) + 2.5 Slot(DR,2) + 6 Slot(DOLR,2)
   + 2.75 Slot(GP,2) + 3.5 Slot(HDD,2) + 4 Slot(ML,2) + 2.75 Slot(BJK,2)
   <= 1000
 floorspace_3: 2 Slot(NB,3) + 2.5 Slot(DR,3) + 6 Slot(DOLR,3)
   + 2.75 Slot(GP,3) + 3.5 Slot(HDD,3) + 4 Slot(ML,3) + 2.75 Slot(BJK,3)
   <= 550
 floorspace_4: 2 Slot(NB,4) + 2.5 Slot(DR,4) + 6 Slot(DOLR,4)
   + 2.75 Slot(GP,4) + 3.5 Slot(HDD,4) + 4 Slot(ML,4) + 2.75 Slot(BJK,4)
   <= 700
 hours: 0.67 Slot(NB,1) + Slot(DR,1) + 2 Slot(DOLR,1) + 1.1 Slot(GP,1)
   + 0.67 Slot(HDD,1) + 0.5 Slot(ML,1) + 0.75 Slot(BJK,1) + 0.67 Slot(NB,2)
   + Slot(DR,2) + 2 Slot(DOLR,2) + 1.1 Slot(GP,2) + 0.67 Slot(HDD,2)
   + 0.5 Slot(ML,2) + 0.75 Slot(BJK,2) + 0.67 Slot(NB,3) + Slot(DR,3)
   + 2 Slot(DOLR,3) + 1.1 Slot(GP,3) + 0.67 Slot(HDD,3) + 0.5 Slot(ML,3)
   + 0.75 Slot(BJK,3) + 0.67 Slot(NB,4) + Slot(DR,4) + 2 Slot(DOLR,4)
   + 1.1 Slot(GP,4) + 0.67 Slot(HDD,4) + 0.5 Slot(ML,4) + 0.75 Slot(BJK,4)
   <= 835
 onhand_NB: Slot(NB,1) + Slot(NB,2) + Slot(NB,3) + Slot(NB,4) <= 200
 onhand_DR: Slot(DR,1) + Slot(DR,2) + Slot(DR,3) + Slot(DR,4) <= 150
 onhand_DOLR: Slot(DOLR,1) + Slot(DOLR,2) + Slot(DOLR,3) + Slot(DOLR,4)
   <= 100
 onhand_GP: Slot(GP,1) + Slot(GP,2) + Slot(GP,3) + Slot(GP,4) <= 250
 onhand_HDD: Slot(HDD,1) + Slot(HDD,2) + Slot(HDD,3) + Slot(HDD,4) <= 125
 onhand_ML: Slot(ML,1) + Slot(ML,2) + Slot(ML,3) + Slot(ML,4) <= 100
 onhand_BJK: Slot(BJK,1) + Slot(BJK,2) + Slot(BJK,3) + Slot(BJK,4) <= 250
Bounds
End

# coding: utf-8

import json
import os
import copy
from StringIO import StringIO
from collections import defaultdict
from datetime import datetime
from traceback import print_exc

import sc2reader
from sc2reader.engine.plugins import APMTracker, ContextLoader, SelectionTracker
from sc2reader.events import PlayerStatsEvent, UnitBornEvent, UnitDiedEvent, UnitDoneEvent, UnitTypeChangeEvent, \
    UpgradeCompleteEvent, GetControlGroupEvent, SetControlGroupEvent, AddToControlGroupEvent

# ======================================================================================================================
# Global variables preset
# ======================================================================================================================

# Source of replay files
source_path = "reps/"

# Used for debugging when filtering out replay files (cleanup)
file_search_stats = [0,0,0,0]
file_search_duplicates_timestamps = []

# ======================================================================================================================
# Main loop over replay files
# ======================================================================================================================

for rep in os.listdir(source_path):
    if os.path.isfile(source_path + rep):

        # Used for debugging when filtering out replay files (cleanup)
        removeFromReps = False

        # Replay file open and read
        fh = open(source_path + rep, 'rb')
        data_1 = StringIO(fh.read())

        # Some extra code here helps catch setup errors
        try:
            replay_file = data_1
        except NameError:
            print('\n'
                  'SETUP ERROR: Please follow the directions to add a .SC2Replay file and use\n'
                  '             "Insert to code" to set the data_1 variable to the resulting bytes.\n'
                  '             You may need to rename the data_* variable.')
            raise

# ======================================================================================================================
# Read the replay file into the sc2reader
# ======================================================================================================================

        replay = sc2reader.load_replay(
            replay_file,
            engine=sc2reader.engine.GameEngine(plugins=[ContextLoader(), APMTracker(), SelectionTracker()]))

        # Print out basic replay data
        print("\n\n****************************\nReplay successfully loaded.\n")
        print (source_path + rep + "\n")
        print("Date: %s" % replay.date)
        print("Map Name: " + replay.map_name)
        print("Match time: " + str(replay.game_length.mins) + ":" + str(replay.game_length.secs))
        print("Frames (at 16fps): " + str(replay.frames))
        for player in replay.players:
            print("%s: %s" % (player.result, player))

# ======================================================================================================================
# Establish some unit and building groups
# ======================================================================================================================

        VESPENE_UNITS = ["Assimilator", "Extractor", "Refinery"]
        SUPPLY_UNITS = ["Overlord", "Overseer", "Pylon", "SupplyDepot"]
        WORKER_UNITS = ["Drone", "Probe", "SCV", "MULE"]
        BASE_UNITS = ["CommandCenter", "Nexus", "Hatchery", "Lair", "Hive", "PlanetaryFortress", "OrbitalCommand"]
        GROUND_UNITS = ["Barracks", "Factory", "GhostAcademy", "Armory", "RoboticsBay", "RoboticsFacility", "TemplarArchive",
                        "DarkShrine", "WarpGate", "SpawningPool", "RoachWarren", "HydraliskDen", "BanelingNest", "UltraliskCavern",
                        "LurkerDen", "InfestationPit"]
        AIR_UNITS = ["Starport", "FusionCore", "RoboticsFacility", "Stargate", "FleetBeacon", "Spire", "GreaterSpire"]
        TECH_UNITS = ["EngineeringBay", "Armory", "GhostAcademy", "TechLab", "FusionCore", "Forge", "CyberneticsCore",
                      "TwilightCouncil", "RoboticsFacility", "RoboticsBay", "FleetBeacon", "TemplarArchive", "DarkShrine",
                      "SpawningPool", "RoachWarren", "HydraliskDen", "BanelingNest", "UltraliskCavern", "LurkerDen", "Spire",
                      "GreaterSpire", "EvolutionChamber", "InfestationPit"]
        ARMY_UNITS = ["Marine", "Colossus", "InfestorTerran", "Baneling", "Mothership", "MothershipCore", "Changeling", "SiegeTank", "Viking", "Reaper",
                      "Ghost", "Marauder", "Thor", "Hellion", "Hellbat", "Cyclone", "Liberator", "Medivac", "Banshee", "Raven", "Battlecruiser", "Nuke", "Zealot",
                      "Stalker", "HighTemplar", "Disruptor", "DarkTemplar", "Sentry", "Phoenix", "Carrier", "Oracle", "VoidRay", "Tempest", "WarpPrism", "Observer",
                      "Immortal", "Adept", "Zergling", "Overlord", "Hydralisk", "Mutalisk", "Ultralisk", "Roach", "Infestor", "Corruptor",
                      "BroodLord", "Queen", "Overseer", "Archon", "Broodling", "InfestedTerran", "Ravager", "Viper", "SwarmHost"]
        ARMY_AIR = ["Mothership", "MothershipCore", "Viking", "Liberator", "Medivac", "Banshee", "Raven", "Battlecruiser",
                    "Viper", "Mutalisk", "Phoenix", "Oracle", "Carrier", "VoidRay", "Tempest", "Observer", "WarpPrism", "BroodLord",
                    "Corruptor", "Observer", "Overseer"]
        ARMY_GROUND = [k for k in ARMY_UNITS if k not in ARMY_AIR]

# ======================================================================================================================
# Establish event parsers
# ======================================================================================================================

        def handle_count(caller, event, key, add_value, start_val=0):
            if len(caller.players[event.unit.owner.pid][key]) == 0:
                caller.players[event.unit.owner.pid][key].append((0, 0))
            # Get the last value
            last_val = caller.players[event.unit.owner.pid][key][-1][1]
            caller.players[event.unit.owner.pid][key].append((event.frame, last_val + add_value))

        def handle_expansion_events(caller, event):
            if type(event) is UnitDoneEvent:
                unit = str(event.unit).split()[0]
                if unit in BASE_UNITS:
                    caller.players[event.unit.owner.pid]["expansion_event"].append((event.frame, "+", unit))
                    handle_count(caller, event, "expansion_buildings", 1, start_val=1)
            elif type(event) is UnitDiedEvent:
                unit = str(event.unit).split()[0]
                if unit in BASE_UNITS:
                    caller.players[event.unit.owner.pid]["expansion_event"].append((event.frame, "-", unit))
                    handle_count(caller, event, "expansion_buildings", -1, start_val=1)
            elif type(event) is UnitTypeChangeEvent:
                if event.unit.name in BASE_UNITS:
                    caller.players[event.unit.owner.pid]["expansion_event"].append((event.frame, "*", event.unit.name))

        def handle_worker_events(caller, event):
            if type(event) is PlayerStatsEvent:
                caller.players[event.pid]["workers_active"].append((event.frame, event.workers_active_count))
            elif type(event) is UnitBornEvent:
                unit = str(event.unit).split()[0]
                if unit in WORKER_UNITS:
                    caller.players[event.control_pid]["worker_event"].append((event.frame, "+", unit))
            elif type(event) is UnitDiedEvent:
                unit = str(event.unit).split()[0]
                if unit in WORKER_UNITS:
                    caller.players[event.unit.owner.pid]["worker_event"].append((event.frame, "-", unit))

        def handle_supply_events(caller, event):
            if type(event) is PlayerStatsEvent:
                caller.players[event.pid]["supply_available"].append((event.frame, int(event.food_made)))
                caller.players[event.pid]["supply_consumed"].append((event.frame, int(event.food_used)))
                utilization = 0 if event.food_made == 0 else event.food_used / event.food_made
                caller.players[event.pid]["supply_utilization"].append((event.frame, utilization))
                utilization_fixed = event.food_used / 200 if event.food_made >= 200 else utilization
                caller.players[event.pid]["supply_utilization_fixed"].append((event.frame, utilization_fixed))
                worker_ratio = 0 if event.food_used == 0 else event.workers_active_count / event.food_used
                caller.players[event.pid]["worker_supply_ratio"].append((event.frame, worker_ratio))
                worker_ratio_fixed = event.workers_active_count / 200 if event.food_made >= 200 else worker_ratio
                caller.players[event.pid]["worker_supply_ratio"].append((event.frame, worker_ratio_fixed))
            elif type(event) is UnitDoneEvent:
                unit = str(event.unit).split()[0]
                if unit in SUPPLY_UNITS:
                    caller.players[event.unit.owner.pid]["supply_event"].append((event.frame, "+", unit))
            elif type(event) is UnitBornEvent:
                # Specifically for Overlord
                unit = str(event.unit).split()[0]
                if unit == "Overlord":
                    caller.players[event.control_pid]["supply_event"].append((event.frame, "+", unit))
            elif type(event) is UnitDiedEvent:
                # Buildings/ Overlord/Overseer
                unit = str(event.unit).split()[0]
                if unit in SUPPLY_UNITS:
                    caller.players[event.unit.owner.pid]["supply_event"].append((event.frame, "-", unit))
            elif type(event) is UnitTypeChangeEvent:
                if event.unit_type_name == "Overseer":
                    caller.players[event.unit.owner.pid]["supply_event"].append((event.frame, "*", event.unit_type_name))

        def handle_vespene_events(caller, event):
            if type(event) is PlayerStatsEvent:
                caller.players[event.pid]["vespene_available"].append((event.frame, event.vespene_current))
                caller.players[event.pid]["vespene_collection_rate"].append((event.frame, event.vespene_collection_rate))
                vesp_per_worker = 0 if event.workers_active_count == 0 else event.vespene_collection_rate / event.workers_active_count
                caller.players[event.pid]["vespene_per_worker_rate"].append((event.frame, vesp_per_worker))
                caller.players[event.pid]["vespene_cost_active_forces"].append((event.frame, event.vespene_used_active_forces))
                caller.players[event.pid]["vespene_spend"].append((event.frame, event.vespene_used_current))
                caller.players[event.pid]["vespene_value_current_technology"].append((event.frame, event.vespene_used_current_technology))
                caller.players[event.pid]["vespene_value_current_army"].append((event.frame, event.vespene_used_current_army))
                caller.players[event.pid]["vespene_value_current_economic"].append((event.frame, event.vespene_used_current_economy))
                caller.players[event.pid]["vespene_queued"].append((event.frame, event.vespene_used_in_progress))
                caller.players[event.pid]["vespene_queued_technology"].append((event.frame, event.vespene_used_in_progress_technology))
                caller.players[event.pid]["vespene_queued_army"].append((event.frame, event.vespene_used_in_progress_technology))
                caller.players[event.pid]["vespene_queued_economic"].append((event.frame, event.vespene_used_in_progress_economy))
            elif type(event) is UnitDoneEvent:
                unit = str(event.unit).split()[0]
                if unit in VESPENE_UNITS:
                    caller.players[event.unit.owner.pid]["vespene_event"].append((event.frame, "+", unit))
            elif type(event) is UnitDiedEvent:
                unit = str(event.unit).split()[0]
                if unit in VESPENE_UNITS:
                    caller.players[event.unit.owner.pid]["vespene_event"].append((event.frame, "-", unit))

        def handle_resources_events(caller, event):
            if type(event) is PlayerStatsEvent:
                caller.players[event.pid]["mineral_destruction"].append((event.frame, event.minerals_killed))
                caller.players[event.pid]["mineral_destruction_army"].append((event.frame, event.minerals_killed_army))
                caller.players[event.pid]["mineral_destruction_economic"].append((event.frame, event.minerals_killed_economy))
                caller.players[event.pid]["mineral_destruction_technology"].append((event.frame, event.minerals_killed_technology))
                caller.players[event.pid]["mineral_loss"].append((event.frame, event.minerals_lost))
                caller.players[event.pid]["mineral_loss_army"].append((event.frame, event.minerals_lost_army))
                caller.players[event.pid]["mineral_loss_economic"].append((event.frame, event.minerals_lost_economy))
                caller.players[event.pid]["mineral_loss_technology"].append((event.frame, event.minerals_lost_technology))

                caller.players[event.pid]["vespene_destruction"].append((event.frame, event.vespene_killed))
                caller.players[event.pid]["vespene_destruction_army"].append((event.frame, event.vespene_killed_army))
                caller.players[event.pid]["vespene_destruction_economic"].append((event.frame, event.vespene_killed_economy))
                caller.players[event.pid]["vespene_destruction_technology"].append((event.frame, event.vespene_killed_technology))
                caller.players[event.pid]["vespene_loss"].append((event.frame, event.vespene_lost))
                caller.players[event.pid]["vespene_loss_army"].append((event.frame, event.vespene_lost_army))
                caller.players[event.pid]["vespene_loss_economic"].append((event.frame, event.vespene_lost_economy))
                caller.players[event.pid]["vespene_loss_technology"].append((event.frame, event.vespene_lost_technology))

        def handle_ground_events(caller, event):
            if type(event) is UnitDoneEvent:
                unit = str(event.unit).split()[0]
                if unit in GROUND_UNITS:
                    count_name = "_".join(["building", unit, "count"])
                    caller.players[event.unit.owner.pid]["ground_building"].append((event.frame, "+", unit))
                    handle_count(caller, event, count_name, 1)
            elif type(event) is UnitDiedEvent:
                unit = str(event.unit).split()[0]
                if unit in GROUND_UNITS:
                    count_name = "_".join(["building", unit, "count"])
                    caller.players[event.unit.owner.pid]["ground_building"].append((event.frame, "-", unit))
                    handle_count(caller, event, count_name, -1)
            elif type(event) is UnitTypeChangeEvent:
                if event.unit_type_name == "LurkerDen":
                    count_name = "_".join(["building", event.unit_type_name, "count"])
                    caller.players[event.unit.owner.pid]["ground_building"].append((event.frame, "*", event.unit_type_name))
                    handle_count(caller, event, count_name, 1)

        def handle_air_events(caller, event):
            if type(event) is UnitDoneEvent:
                unit = str(event.unit).split()[0]
                if unit in AIR_UNITS:
                    count_name = "_".join(["building", unit, "count"])
                    caller.players[event.unit.owner.pid]["air_building"].append((event.frame, "+", unit))
                    handle_count(caller, event, count_name, 1)
            elif type(event) is UnitDiedEvent:
                unit = str(event.unit).split()[0]
                if unit in AIR_UNITS:
                    count_name = "_".join(["building", unit, "count"])
                    caller.players[event.unit.owner.pid]["air_building"].append((event.frame, "-", unit))
                    handle_count(caller, event, count_name, -1)
            elif type(event) is UnitTypeChangeEvent:
                if event.unit_type_name == "GreaterSpire":
                    count_name = "_".join(["building", event.unit_type_name, "count"])
                    caller.players[event.unit.owner.pid]["air_building"].append((event.frame, "*", event.unit_type_name))
                    handle_count(caller, event, count_name, 1)

        def handle_unit_events(caller, event):
            if type(event) is UnitBornEvent:
                unit = event.unit_type_name
                if unit in ARMY_UNITS:
                    unit_count_name = "_".join(["unit", unit, "count"])
                    caller.players[event.control_pid]["army_event"].append((event.frame, "+", unit))
                    handle_count(caller, event, unit_count_name, 1)
                    if unit in ARMY_AIR:
                        handle_count(caller, event, "army_air", 1)
                    elif unit in ARMY_GROUND:
                        handle_count(caller, event, "army_ground", 1)
                    handle_count(caller, event, "army_count", 1)
            elif type(event) is UnitDoneEvent:
                unit = str(event.unit).split()[0]
                if unit in ARMY_UNITS:
                    unit_count_name = "_".join(["unit", unit, "count"])
                    caller.players[event.unit.owner.pid]["army_event"].append((event.frame, "+", unit))
                    handle_count(caller, event, unit_count_name, 1)
                    if unit in ARMY_AIR:
                        handle_count(caller, event, "army_air", 1)
                    elif unit in ARMY_GROUND:
                        handle_count(caller, event, "army_air", 1)
                    handle_count(caller, event, "army_count", 1)
            elif type(event) is UnitDiedEvent:
                unit = str(event.unit).split()[0]
                if unit in ARMY_UNITS:
                    unit_count_name = "_".join(["unit", unit, "count"])
                    caller.players[event.unit.owner.pid]["army_event"].append((event.frame, "-", unit))
                    if unit in ARMY_AIR:
                        handle_count(caller, event, "army_air", -1)
                    elif unit in ARMY_GROUND:
                        handle_count(caller, event, "army_ground", -1)
                    handle_count(caller, event, unit_count_name, -1)
                    handle_count(caller, event, "army_count", -1)
            elif type(event) is UnitTypeChangeEvent:
                unit = str(event.unit).split()[0]
                if event.unit_type_name in ARMY_UNITS:
                    unit_count_name = "_".join(["unit", event.unit_type_name, "count"])

                    caller.players[event.unit.owner.pid]["army_event"].append((event.frame, "*", unit))

                    handle_count(caller, event, unit_count_name, 1)

        def handle_tech_events(caller, event):
            if type(event) is UnitDoneEvent:
                unit = str(event.unit).split()[0]
                if unit in TECH_UNITS:
                    caller.players[event.unit.owner.pid]["tech_building"].append((event.frame, "+", unit))
            elif type(event) is UnitDiedEvent:
                unit = str(event.unit).split()[0]
                if unit in TECH_UNITS:
                    caller.players[event.unit.owner.pid]["tech_building"].append((event.frame, "-", unit))
            elif type(event) is UnitTypeChangeEvent:
                if event.unit_type_name in ["GreaterSpire", "LurkerDen"]:
                    caller.players[event.unit.owner.pid]["tech_building"].append((event.frame, "*", event.unit_type_name))

        def handle_upgrade_events(caller, event):
            if type(event) is UpgradeCompleteEvent and event.frame > 0:
                if not event.upgrade_type_name.startswith("Spray"):
                    caller.players[event.pid]["upgrades"].append((event.frame, event.upgrade_type_name))

        def handle_mineral_events(caller, event):
            if type(event) is PlayerStatsEvent:
                caller.players[event.pid]["minerals_available"].append((event.frame, event.minerals_current))
                caller.players[event.pid]["mineral_collection_rate"].append((event.frame, event.minerals_collection_rate,))
                caller.players[event.pid]["mineral_cost_active_forces"].append((event.frame, event.minerals_used_active_forces))
                mins_per_worker = 0 if event.workers_active_count == 0 else event.minerals_collection_rate / event.workers_active_count
                caller.players[event.pid]["mineral_per_worker_rate"].append((event.frame, mins_per_worker))
                caller.players[event.pid]["mineral_spend"].append((event.frame, event.minerals_used_current))
                caller.players[event.pid]["mineral_value_current_technology"].append((event.frame, event.minerals_used_current_technology))
                caller.players[event.pid]["mineral_value_current_army"].append((event.frame, event.minerals_used_current_army))
                caller.players[event.pid]["mineral_value_current_economic"].append((event.frame, event.minerals_used_current_economy))
                caller.players[event.pid]["mineral_queued"].append((event.frame, event.minerals_used_in_progress))
                caller.players[event.pid]["mineral_queued_technology"].append((event.frame, event.minerals_used_in_progress_technology))
                caller.players[event.pid]["mineral_queued_army"].append((event.frame, event.minerals_used_in_progress_army))
                caller.players[event.pid]["mineral_queued_economic"].append((event.frame, event.minerals_used_in_progress_economy))

        def handle_hotkeys_events(caller,event):
            if type(event) is GetControlGroupEvent:
                caller.players[event.pid]["hotkey_used"].append((event.frame, event.hotkey))
            if type(event) is AddToControlGroupEvent:
                caller.players[event.pid]["hotkey_add"].append((event.frame, event.hotkey))
            if type(event) is SetControlGroupEvent:
                caller.players[event.pid]["hotkey_set"].append((event.frame, event.hotkey))

# ======================================================================================================================
# Aggregate all of our event parsers for use by our ReplayData class
# ======================================================================================================================

        handlers = [handle_expansion_events, handle_worker_events, handle_supply_events, handle_mineral_events,
                    handle_vespene_events, handle_ground_events, handle_air_events, handle_tech_events, handle_upgrade_events,
                    handle_unit_events]# , handle_hotkeys_events]

# ======================================================================================================================
# ReplayData class to structure and process replay files
# ======================================================================================================================

        class ReplayData:
            __parsers__ = handlers

            @classmethod
            def parse_replay(cls, replay=None, replay_file=None, file_object=None):

                replay_data = ReplayData(replay_file)
                try:
                    # This is the engine that holds some required plugins for parsing
                    engine = sc2reader.engine.GameEngine(plugins=[ContextLoader(), APMTracker(), SelectionTracker()])

                    if replay:
                        pass
                    elif replay_file and not file_object:
                        # Then we are not using ObjectStorage for accessing replay files
                        replay = sc2reader.load_replay(replay_file, engine=engine)
                    elif file_object:
                        # We are using ObjectStorage to access replay files
                        replay = sc2reader.load_replay(file_object, engine=engine)
                    else:
                        pass  # TODO: fix this logic

                    # Get the number of frames (one frame is 1/16 of a second)
                    replay_data.frames = replay.frames
                    # Gets the game mode (if available)
                    replay_data.game_mode = replay.real_type
                    # Gets the map hash (if we want to download the map, or do map-based analysis)
                    replay_data.map_hash = replay.map_hash

                    # Use the parsers to get data
                    for event in replay.events:
                        for parser in cls.__parsers__:
                            parser(replay_data, event)

                    # Check if there was a winner
                    if replay.winner is not None:
                        replay_data.winners = replay.winner.players
                        replay_data.losers = [p for p in replay.players if p not in replay.winner.players]
                    else:
                        replay_data.winners = []
                        replay_data.losers = []
                    # Check to see if expansion data is available
                    # replay_data.expansion = replay.expasion
                    return replay_data
                except:
                    # Print our error and return NoneType object
                    print_exc()
                    return None

            def as_dict(self):
                return {
                    "processed_on": datetime.utcnow().isoformat(),
                    "replay_name": self.replay,
                    "expansion": self.expansion,
                    "frames": self.frames,
                    "mode": self.game_mode,
                    "map": self.map_hash,
                    "matchup": "v".join(sorted([s.detail_data["race"][0].upper() for s in self.winners + self.losers])),
                    "winners": [(s.pid, s.name, s.detail_data['race']) for s in self.winners],
                    "losers": [(s.pid, s.name, s.detail_data['race']) for s in self.losers],
                    "stats_names": [k for k in self.players[1].keys()],
                    "stats": {player: data for player, data in self.players.items()}
                }

            def __init__(self, replay):
                self.players = defaultdict(lambda: defaultdict(list))
                self.replay = replay
                self.winners = []
                self.losers = []
                self.expansion = None

        replay_object = ReplayData.parse_replay(replay=replay)
        replay_dict = replay_object.as_dict()

# ======================================================================================================================
# Build the basic output from rep file
# ======================================================================================================================

        playerNamesAndPids = {'glabII':'302','glabIII':'303','glabIV':'304','glabV': '305?','glabVI': '306','glabVII':'305',
                            'glabVIII': '308','glabIX':'309','glabX': '310','glabXI':'307','glabXII':'312','glabXIII':'313?',
                            'glabXIV':'314','glabXV':'315','glabXVI':'316','glabXVII':'317','glabXVIII':'318','glabXIX':'319',
                            'glabXX':'320','glabXXI':'321','glabXXII':'322','glabXXIII':'323','glabXXIV':'324','glabXXV':'325',
                            'glabXXVI':'326','glabXXVII':'327','glabXXVIII':'328','glabXXIX':'329','glabXXX':'330','glabXXXI':'331?',
                            'glabXXXII':'332','glabXXXIII':'333?','glabXXXIV':'334','glabXXXV':'335','glabXXXVI':'336','glabXXXVII':'337?',
                            'glabXXXVIII':'338','glXXXVIII':'338?','glabXXXIX':'339','glabXL' : '340?','glabXLI':'341',
                            'glabXLII':'342','glabXLIII':'343','glabXLIV':'344?','glabXLV':'345','glabXLVI':'346','glabXLVII':'347',
                            'glabXLVIII':'348?','glabXLIX':'349','glabL':'350','glabLI':'351','glabLII':'352','glabLIII':'353',
                            'glabLIV':'354','glabLV':'355','glabLVI':'356','glabLVII':'357','glabLVIII':'358','glabLVIX':'359',
                            'glabLIX': '359?','glabLX':'360','glabLXI': '361','glabLXII':'362','glabLXIII':'363','glabLXIV':'364',
                            'glabLXV':'365','glabLXVI':'366','glabLXVII':'367?','glabLXVIII': '368','glabLXIX': '301','glabLXX': '370',
                            'glabLXXVI': '371?','glabLXXVII': '372?','glabLXXIII': '373?','glabLXXIV': '374?','glabLXXV': '344',}

        # Main dict for storage of exported data
        output_basic = {}
        replay_data = copy.deepcopy(replay_object)

        # Identify the player
        # ==============================================================================================================
        output_basic['fileID'] = fh.name
        output_basic['playerName'] = replay.humans[0].name.encode('utf-8')

        # If the player's nickname can not be converted to PId, use original value
        # ==============================================================================================================
        if replay.humans[0].name.encode('utf-8') in playerNamesAndPids:
            output_basic['participantId'] = playerNamesAndPids[replay.humans[0].name.encode('utf-8')]
        else:
            output_basic['participantId'] = replay.humans[0].name.encode('utf-8')

        # Check if match has a result
        # ==============================================================================================================
        if replay.humans[0].result is not None:
            output_basic['matchResult'] = replay.humans[0].result.encode('utf-8')
        else:
            output_basic['matchResult'] = "None".encode('utf-8')

        # APMs
        # ==============================================================================================================
        output_basic['avgAPM'] = replay.humans[0].avg_apm

        # Global match params
        # ==============================================================================================================
        output_basic['matchStartDateTime'] = replay.start_time.isoformat()
        output_basic['matchEndDateTime'] = replay.end_time.isoformat()
        output_basic['matchLengthRealTime'] = replay.game_length.seconds
        output_basic['matchLength'] = replay.game_length.seconds * 1.4
        output_basic['matchMapName'] = replay.map_name.encode('utf-8')

        # Check if match was set up with an AI opponent
        # ==============================================================================================================
        if len(replay.computers) > 0:
            output_basic['aiDifficulty'] = replay.computers[0].difficulty.encode('utf-8')
            output_basic['aiRace'] = replay.computers[0].play_race.encode('utf-8')
            output_basic['aiBuild'] = replay.computers[0].slot_data['ai_build']
            output_basic['aiHandicap'] = replay.computers[0].slot_data['handicap']

        # Group hot keys usage
        # ==============================================================================================================
        if len(replay_data.players) == 3:
            output_basic['hotkeysUsageTotal'] = len(replay_data.players[0]['hotkey_used'])
            output_basic['hotkeysUsageIntensity'] = output_basic['hotkeysUsageTotal'] / output_basic['matchLength']
            output_basic['hotkeysSetaddTotal'] = len(replay_data.players[0]['hotkey_add']) + len(replay_data.players[0]['hotkey_set'])
            output_basic['hotkeysSetaddIntensity'] = output_basic['hotkeysSetaddTotal'] / output_basic['matchLength']

        # 'timelimit' variables value is set at 5, 10 and 15 minutes of game time.
        # PlayerStatsEvent are updated every 10 sec of real time, position 126 was calculated by multiplying 900 sec.
        # by 1.4 - the game speed value and dividing the result by 10 - the update rate
        timelimit_2 = 21
        timelimit_5 = 42
        timelimit_10 = 84
        timelimit_15 = 126

        # Supply usage
        # ==============================================================================================================
        tmp_average_su = 0.0
        tmp_limit_su = 0

        for idx, val in enumerate(replay_data.players[1]['supply_utilization_fixed']):
            tmp_average_su += val[1]
            tmp_limit_su += 1.0 if val[1] == 1.0 else 0.0

            if idx == timelimit_2:
                output_basic['supplyAverage_2'] = tmp_average_su / idx
                output_basic['supplyLimit_2'] = tmp_limit_su / idx
            if idx == timelimit_5:
                output_basic['supplyAverage_5'] = tmp_average_su / idx
                output_basic['supplyLimit_5'] = tmp_limit_su / idx
            if idx == timelimit_10:
                output_basic['supplyAverage_10'] = tmp_average_su / idx
                output_basic['supplyLimit_10'] = tmp_limit_su / idx
            if idx == timelimit_15:
                output_basic['supplyAverage_15'] = tmp_average_su / idx
                output_basic['supplyLimit_15'] = tmp_limit_su / idx

            if idx+1 == len(replay_data.players[1]['supply_utilization_fixed']):
                output_basic['supplyAverageMatch'] = tmp_average_su / idx
                output_basic['supplyLimitMatch'] = tmp_limit_su / idx

        output_basic['supplyMaxTime'] = 0
        output_basic['supplyMaxValue'] = 0
        tmp_max_hit = False

        for idx, val in enumerate(replay_data.players[1]['supply_available']):
            if val[1] > output_basic['supplyMaxValue']:
                output_basic['supplyMaxValue'] = val[1]
            if val[1] >= 200 and not tmp_max_hit:
                output_basic['supplyMaxTime'] = val[0]
                tmp_max_hit = True

        # Resources mining and usage
        # ==============================================================================================================
        data_src_list = ['mineral_collection_rate','vespene_collection_rate','minerals_available','mineral_spend','vespene_spend']

        variables_list = [
            # vars 2
            ['mineralCollectionRateAverage_2', 'vespeneCollectionRateAverage_2',
             'mineralAvailableAverage_2', 'mineralSpendAverage_2', 'vespeneSpendAverage_2'],
            # vars 5
            ['mineralCollectionRateAverage_5', 'vespeneCollectionRateAverage_5', 'mineralAvailableAverage_5',
             'mineralSpendAverage_5', 'vespeneSpendAverage_5'],
            # vars 10
            ['mineralCollectionRateAverage_10', 'vespeneCollectionRateAverage_10',
             'mineralAvailableAverage_10', 'mineralSpendAverage_10', 'vespeneSpendAverage_10'],
            # vars 15
            ['mineralCollectionRateAverage_15', 'vespeneCollectionRateAverage_15',
             'mineralAvailableAverage_15', 'mineralSpendAverage_15', 'vespeneSpendAverage_15']]

        # initiate 'output_basic' with 'variables_list'
        for var_list in variables_list:
            for var in var_list:
                output_basic[var] = None

        resource = 0

        while resource < len(data_src_list):

            tmp_list_name = data_src_list[resource]
            tmp_average = 0
            tmp_list = copy.deepcopy(replay_data.players)

            for idx, val in enumerate(tmp_list[1][tmp_list_name]):
                tmp_average += val[1]

                if idx == timelimit_2:
                    output_basic[variables_list[0][resource]] = tmp_average / idx
                if idx == timelimit_5:
                    output_basic[variables_list[1][resource]] = tmp_average / idx
                if idx == timelimit_10:
                    output_basic[variables_list[2][resource]] = tmp_average / idx
                if idx == timelimit_15:
                    output_basic[variables_list[3][resource]] = tmp_average / idx

            resource += 1

        # TODO: refactor the summary section set None if value 0.0

        if output_basic['mineralCollectionRateAverage_2'] is not None:
            output_basic['resourcesCollectionRateAverage_2'] = output_basic['mineralCollectionRateAverage_2'] + output_basic['vespeneCollectionRateAverage_2']
        if output_basic['mineralCollectionRateAverage_5'] is not None:
            output_basic['resourcesCollectionRateAverage_5'] = output_basic['mineralCollectionRateAverage_5'] + output_basic['vespeneCollectionRateAverage_5']
        if output_basic['mineralCollectionRateAverage_10'] is not None:
            output_basic['resourcesCollectionRateAverage_10'] = output_basic['mineralCollectionRateAverage_10'] + output_basic['vespeneCollectionRateAverage_10']
        if output_basic['mineralCollectionRateAverage_15'] is not None:
            output_basic['resourcesCollectionRateAverage_15'] = output_basic['mineralCollectionRateAverage_15'] + output_basic['vespeneCollectionRateAverage_15']

        if output_basic['mineralSpendAverage_2'] is not None:
            output_basic['resourcesSpendAverage_2'] = output_basic['mineralSpendAverage_2'] + output_basic['vespeneSpendAverage_2']
        if output_basic['mineralSpendAverage_5'] is not None:
            output_basic['resourcesSpendAverage_5'] = output_basic['mineralSpendAverage_5'] + output_basic['vespeneSpendAverage_5']
        if output_basic['mineralSpendAverage_10'] is not None:
            output_basic['resourcesSpendAverage_10'] = output_basic['mineralSpendAverage_10'] + output_basic['vespeneSpendAverage_10']
        if output_basic['mineralSpendAverage_15'] is not None:
            output_basic['resourcesSpendAverage_15'] = output_basic['mineralSpendAverage_15'] + output_basic['vespeneSpendAverage_15']

        output_basic['mineralPeakCollectionRate'] = 0
        output_basic['vespenePeakCollectionRate'] = 0

        data_src_list = ['mineral_collection_rate', 'vespene_collection_rate']
        resource = 0

        while resource < len(data_src_list):

            tmp_list_name = data_src_list[resource]
            tmp_average = 0
            tmp_list = copy.deepcopy(replay_data.players)

            for idx, val in enumerate(tmp_list[1][tmp_list_name]):

                if resource == 0:
                    if val[1] > output_basic['mineralPeakCollectionRate']:
                        output_basic['mineralPeakCollectionRate'] = val[1]
                else:
                    if val[1] > output_basic['vespenePeakCollectionRate']:
                        output_basic['vespenePeakCollectionRate'] = val[1]

            resource += 1

        output_basic['resourcesPeakCollectionRate'] = output_basic['mineralPeakCollectionRate'] + output_basic['vespenePeakCollectionRate']

        # Expansion
        # ==============================================================================================================
        tmp_expandCC_search = []

        for i, v in enumerate(replay_data.players[1]['expansion_event']):
            if v[2] == 'CommandCenter' or 'OrbitalCommand':
                if v[1] == '+':
                    tmp_expandCC_search.append(i)

        tmp_upgradeCC_search = []

        for i, v in enumerate(replay_data.players[1]['expansion_event']):
            if v[2] == 'OrbitalCommand':
                if v[1] == '*':
                    tmp_upgradeCC_search.append(i)

        for idx, val in enumerate(tmp_expandCC_search):
            output_basic['expansionCC_'+str((idx+2))] = replay_data.players[1]['expansion_event'][val][0] / 16 / 1.4

        for idx, val in enumerate(tmp_upgradeCC_search):
            output_basic['upgradeCC_'+str((idx+1))] = replay_data.players[1]['expansion_event'][val][0] / 16 / 1.4

        # TODO: minute by minute for supply, minerals, vespen, army value

        # Convert dict to JSON for MongoDB insertion
        json_output = json.dumps(output_basic)

# ======================================================================================================================
# Import data output to the database (MongoDB)
# ======================================================================================================================

        from pymongo import MongoClient

        # Establish connection
        client = MongoClient('localhost', 27017)
        db = client.sc2reps

        # Mongo DBs desc:
        # basic_db - from Feb 2018, first take, corrected pIDs, no duplicates
        # basic_db_rev2 - test to check data migration from basic_db, unused (not in basic_db) files removed
        # basic_db_rev21 - duplicate files removed
        # basic_db_rev22 -

        collection_src = db.basic_db
        collection_tgt = db.basic_db_rev22

        # Compare processed file timestamp with collection timestamp
        # If there is one, make sure it's the only one
        find_result = collection_src.find({'matchStartDateTime':output_basic['matchStartDateTime']})

        if find_result.count() == 0:
            file_search_stats[0] += 1
            #removeFromReps = True
        elif find_result.count() > 0:
            output_basic['participantId'] = find_result[0]['participantId']
            output_basic['playerName'] = find_result[0]['playerName']
            if find_result.count() == 1:
                file_search_stats[1] += 1
                object_id = collection_tgt.insert_one(output_basic).inserted_id
            elif find_result.count() == 2:
                file_search_stats[2] += 1
                if output_basic['matchStartDateTime'] not in file_search_duplicates_timestamps:
                    file_search_duplicates_timestamps.append(output_basic['matchStartDateTime'])
                    object_id = collection_tgt.insert_one(output_basic).inserted_id
                else:
                    removeFromReps = True

        # Output data imported to the DB at this point, release file handle
        fh.close()
        print ("File handle released, bye\n****************************")

        if removeFromReps:
            #os.remove(fh.name)
            print ("File removed.")

print (file_search_stats)
print ("All went well")

# EOF
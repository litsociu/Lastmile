# data_model.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Any
import math


@dataclass
class Instance:
    customers: Set[str]
    vehicles: List[str]
    depots: Dict[str, str]               # vehicle_id -> depot_id
    depot_capacity: Dict[str, float]     # storage cap

    distance: Dict[str, Dict[str, float]]
    travel_time: Dict[str, Dict[str, float]]
    road_allowed: Dict[str, Dict[str, Dict[str, int]]]

    demand_w: Dict[str, float]
    demand_v: Dict[str, float]
    service_time: Dict[str, float]
    tw_start: Dict[str, float]
    tw_end: Dict[str, float]
    priority: Dict[str, int]
    delivery_type: Dict[str, str]
    coords: Dict[str, Tuple[float,float]]
    customer_cluster: Dict[str, str]

    vehicle_cap_w: Dict[str, float]
    vehicle_cap_v: Dict[str, float]
    shift_max: Dict[str, float]
    max_distance: Dict[str, float]
    fixed_cost: Dict[str, float]
    var_cost: Dict[str, float]

    penalty_unserved: Dict[str, float]
    lambda_E: Dict[str, float]
    lambda_L: Dict[str, float]
    lambda_H: Dict[str, float]

    lambda_W: float
    lambda_dist_overtime: float
    lambda_depot_capacity: float

    BIG_CAP: float = 1e6
    BIG_ROAD: float = 1e6


@dataclass
class Route:
    vehicle_id: str
    stops: List[str]   # [depot, c1, c2, ..., depot]

    def copy(self) -> "Route":
        return Route(vehicle_id=self.vehicle_id, stops=list(self.stops))


@dataclass
class Solution:
    routes: Dict[str, Route]
    all_customers: Set[str]
    objective: float = math.inf
    meta: Dict[str, Any] = field(default_factory=dict)

    def copy(self) -> "Solution":
        return Solution(
            routes={k: r.copy() for k, r in self.routes.items()},
            all_customers=set(self.all_customers),
            objective=self.objective,
            meta={k: v for k, v in self.meta.items()},
        )

import random
import time
import threading
import json
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
import queue
import logging
from collections import defaultdict

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class NodeRole(Enum):
    PROPOSER = "proposer"
    ACCEPTOR = "acceptor"
    LEARNER = "learner"

@dataclass
class PrepareRequest:
    proposal_number: int
    proposer_id: str

@dataclass
class PrepareResponse:
    proposal_number: int
    promised: bool
    accepted_proposal: Optional[int] = None
    accepted_value: Optional[str] = None
    acceptor_id: str = ""

@dataclass
class AcceptRequest:
    proposal_number: int
    value: str
    proposer_id: str

@dataclass
class AcceptResponse:
    proposal_number: int
    accepted: bool
    acceptor_id: str

@dataclass
class Proposal:
    number: int
    value: str
    proposer_id: str
    timestamp: float

class PaxosNode:
    def __init__(self, node_id: str, peers: List[str], cluster=None):
        self.node_id = node_id
        self.peers = peers
        self.cluster = cluster
        
        # Paxos state for Proposer
        self.proposal_number = 0
        self.current_proposal: Optional[Proposal] = None
        
        # Paxos state for Acceptor
        self.promised_proposal = -1  # Highest proposal number promised
        self.accepted_proposal = -1  # Highest proposal number accepted
        self.accepted_value: Optional[str] = None
        
        # Paxos state for Learner
        self.learned_values: Dict[int, str] = {}  # proposal_number -> value
        self.consensus_value: Optional[str] = None
        
        # Network simulation
        self.is_active = True
        self.network_delay = random.uniform(0.1, 0.3)  # Simulate network delay
        
        # Logging
        self.logger = logging.getLogger(f"Paxos-{node_id}")
        self.execution_log: List[str] = []
        
        # Statistics
        self.prepare_requests_sent = 0
        self.prepare_responses_received = 0
        self.accept_requests_sent = 0
        self.accept_responses_received = 0
        self.proposals_successful = 0
        self.proposals_failed = 0
        
        self.logger.info(f"Paxos Node {node_id} initialized")
        self._log_event(f"Node {node_id} initialized as Proposer/Acceptor/Learner")

    def _log_event(self, event: str):
        """Registra un evento en el log de ejecución"""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        log_entry = f"[{timestamp}] {event}"
        self.execution_log.append(log_entry)

    def _generate_proposal_number(self) -> int:
        """Genera un número de propuesta único"""
        # Use timestamp + node_id hash to ensure uniqueness
        base_number = int(time.time() * 1000000) % 1000000
        node_hash = hash(self.node_id) % 1000
        self.proposal_number = base_number * 1000 + node_hash
        return self.proposal_number

    def simulate_failure(self):
        """Simula el fallo del nodo"""
        self.is_active = False
        self.logger.warning(f"Node {self.node_id} has FAILED")
        self._log_event(f"Node {self.node_id} FAILED - no longer responding to requests")

    def recover_from_failure(self):
        """Recupera el nodo del fallo"""
        self.is_active = True
        self.logger.info(f"Node {self.node_id} has RECOVERED")
        self._log_event(f"Node {self.node_id} RECOVERED - now responding to requests")

    def propose_value(self, value: str) -> bool:
        """Inicia el algoritmo Paxos para proponer un valor"""
        if not self.is_active:
            self.logger.warning(f"Cannot propose: node {self.node_id} is inactive")
            return False

        proposal_number = self._generate_proposal_number()
        self.current_proposal = Proposal(proposal_number, value, self.node_id, time.time())
        
        self.logger.info(f"Starting Paxos consensus for value '{value}' with proposal {proposal_number}")
        self._log_event(f"PROPOSER: Starting consensus for value '{value}' with proposal #{proposal_number}")
        
        # Phase 1: Prepare
        if self._phase1_prepare():
            # Phase 2: Accept
            return self._phase2_accept()
        else:
            self.proposals_failed += 1
            self._log_event(f"PROPOSER: Proposal #{proposal_number} FAILED in Phase 1")
            return False

    def _phase1_prepare(self) -> bool:
        """Fase 1: Prepare - Envía prepare requests a los acceptors"""
        if not self.current_proposal:
            return False
            
        proposal_number = self.current_proposal.number
        self.logger.info(f"Phase 1: Sending PREPARE({proposal_number}) to acceptors")
        self._log_event(f"PHASE 1: Sending PREPARE({proposal_number}) to all acceptors")
        
        prepare_request = PrepareRequest(proposal_number, self.node_id)
        responses: List[PrepareResponse] = []
        
        # Send prepare to all acceptors (including self)
        for peer_id in self.peers:
            self.prepare_requests_sent += 1
            response = self._send_prepare_request(peer_id, prepare_request)
            if response:
                responses.append(response)
                self.prepare_responses_received += 1
        
        # Check if majority promised
        promises = [r for r in responses if r.promised]
        majority_size = (len(self.peers) // 2) + 1
        
        self.logger.info(f"Phase 1: Received {len(promises)} promises out of {len(responses)} responses (need {majority_size})")
        self._log_event(f"PHASE 1: Received {len(promises)} promises out of {len(responses)} responses")
        
        if len(promises) >= majority_size:
            # Find highest accepted proposal among promises
            highest_accepted = None
            for promise in promises:
                if (promise.accepted_proposal is not None and 
                    promise.accepted_value is not None and
                    (highest_accepted is None or promise.accepted_proposal > highest_accepted[0])):
                    highest_accepted = (promise.accepted_proposal, promise.accepted_value)
            
            if highest_accepted:
                # Use the value from the highest accepted proposal
                old_value = self.current_proposal.value
                self.current_proposal.value = highest_accepted[1]
                self.logger.info(f"Phase 1: Adopting previously accepted value '{highest_accepted[1]}' from proposal {highest_accepted[0]}")
                self._log_event(f"PHASE 1: Adopted value '{highest_accepted[1]}' from higher proposal #{highest_accepted[0]} (original: '{old_value}')")
            
            self._log_event(f"PHASE 1: SUCCESS - Got majority promises, proceeding to Phase 2")
            return True
        else:
            self.logger.warning(f"Phase 1: Failed to get majority promises")
            self._log_event(f"PHASE 1: FAILED - Only got {len(promises)} promises, need {majority_size}")
            return False

    def _phase2_accept(self) -> bool:
        """Fase 2: Accept - Envía accept requests a los acceptors"""
        if not self.current_proposal:
            return False
            
        proposal_number = self.current_proposal.number
        value = self.current_proposal.value
        
        self.logger.info(f"Phase 2: Sending ACCEPT({proposal_number}, '{value}') to acceptors")
        self._log_event(f"PHASE 2: Sending ACCEPT({proposal_number}, '{value}') to all acceptors")
        
        accept_request = AcceptRequest(proposal_number, value, self.node_id)
        responses: List[AcceptResponse] = []
        
        # Send accept to all acceptors (including self)
        for peer_id in self.peers:
            self.accept_requests_sent += 1
            response = self._send_accept_request(peer_id, accept_request)
            if response:
                responses.append(response)
                self.accept_responses_received += 1
        
        # Check if majority accepted
        accepts = [r for r in responses if r.accepted]
        majority_size = (len(self.peers) // 2) + 1
        
        self.logger.info(f"Phase 2: Received {len(accepts)} accepts out of {len(responses)} responses (need {majority_size})")
        self._log_event(f"PHASE 2: Received {len(accepts)} accepts out of {len(responses)} responses")
        
        if len(accepts) >= majority_size:
            self.proposals_successful += 1
            self.consensus_value = value
            self.learned_values[proposal_number] = value
            
            self.logger.info(f"Phase 2: SUCCESS - Value '{value}' has been chosen by consensus!")
            self._log_event(f"PHASE 2: SUCCESS - CONSENSUS ACHIEVED for value '{value}' with proposal #{proposal_number}")
            self._log_event(f"LEARNER: Learned consensus value '{value}'")
            
            # Notify other learners
            self._notify_learners(proposal_number, value)
            return True
        else:
            self.proposals_failed += 1
            self.logger.warning(f"Phase 2: Failed to get majority accepts")
            self._log_event(f"PHASE 2: FAILED - Only got {len(accepts)} accepts, need {majority_size}")
            return False

    def _send_prepare_request(self, peer_id: str, request: PrepareRequest) -> Optional[PrepareResponse]:
        """Simula el envío de una prepare request"""
        if self.cluster:
            peer = self.cluster.get_node(peer_id)
            if peer and peer.is_active:
                # Simulate network delay
                time.sleep(random.uniform(0.01, 0.05))
                return peer._handle_prepare_request(request)
        return None

    def _send_accept_request(self, peer_id: str, request: AcceptRequest) -> Optional[AcceptResponse]:
        """Simula el envío de una accept request"""
        if self.cluster:
            peer = self.cluster.get_node(peer_id)
            if peer and peer.is_active:
                # Simulate network delay
                time.sleep(random.uniform(0.01, 0.05))
                return peer._handle_accept_request(request)
        return None

    def _handle_prepare_request(self, request: PrepareRequest) -> PrepareResponse:
        """Maneja una prepare request (rol de Acceptor)"""
        if not self.is_active:
            return PrepareResponse(request.proposal_number, False, acceptor_id=self.node_id)
        
        self.logger.info(f"ACCEPTOR: Received PREPARE({request.proposal_number}) from {request.proposer_id}")
        self._log_event(f"ACCEPTOR: Received PREPARE({request.proposal_number}) from {request.proposer_id}")
        
        if request.proposal_number > self.promised_proposal:
            # Promise to not accept any proposal with lower number
            old_promised = self.promised_proposal
            self.promised_proposal = request.proposal_number
            
            response = PrepareResponse(
                proposal_number=request.proposal_number,
                promised=True,
                accepted_proposal=self.accepted_proposal if self.accepted_proposal >= 0 else None,
                accepted_value=self.accepted_value,
                acceptor_id=self.node_id
            )
            
            self.logger.info(f"ACCEPTOR: PROMISED proposal {request.proposal_number} (was {old_promised})")
            self._log_event(f"ACCEPTOR: PROMISED proposal #{request.proposal_number} (previous promise: #{old_promised})")
            
            if response.accepted_proposal is not None:
                self._log_event(f"ACCEPTOR: Returning previously accepted proposal #{response.accepted_proposal} with value '{response.accepted_value}'")
            
            return response
        else:
            self.logger.info(f"ACCEPTOR: REJECTED prepare {request.proposal_number} (already promised {self.promised_proposal})")
            self._log_event(f"ACCEPTOR: REJECTED PREPARE({request.proposal_number}) - already promised #{self.promised_proposal}")
            return PrepareResponse(request.proposal_number, False, acceptor_id=self.node_id)

    def _handle_accept_request(self, request: AcceptRequest) -> AcceptResponse:
        """Maneja una accept request (rol de Acceptor)"""
        if not self.is_active:
            return AcceptResponse(request.proposal_number, False, self.node_id)
        
        self.logger.info(f"ACCEPTOR: Received ACCEPT({request.proposal_number}, '{request.value}') from {request.proposer_id}")
        self._log_event(f"ACCEPTOR: Received ACCEPT({request.proposal_number}, '{request.value}') from {request.proposer_id}")
        
        if request.proposal_number >= self.promised_proposal:
            # Accept the proposal
            self.accepted_proposal = request.proposal_number
            self.accepted_value = request.value
            
            self.logger.info(f"ACCEPTOR: ACCEPTED proposal {request.proposal_number} with value '{request.value}'")
            self._log_event(f"ACCEPTOR: ACCEPTED proposal #{request.proposal_number} with value '{request.value}'")
            
            return AcceptResponse(request.proposal_number, True, self.node_id)
        else:
            self.logger.info(f"ACCEPTOR: REJECTED accept {request.proposal_number} (promised {self.promised_proposal})")
            self._log_event(f"ACCEPTOR: REJECTED ACCEPT({request.proposal_number}) - promised #{self.promised_proposal}")
            return AcceptResponse(request.proposal_number, False, self.node_id)

    def _notify_learners(self, proposal_number: int, value: str):
        """Notifica a todos los learners sobre el valor consensuado"""
        if not self.cluster:
            return
            
        for peer_id in self.peers:
            if peer_id != self.node_id:
                peer = self.cluster.get_node(peer_id)
                if peer and peer.is_active:
                    peer._learn_value(proposal_number, value)

    def _learn_value(self, proposal_number: int, value: str):
        """Aprende un valor consensuado (rol de Learner)"""
        if not self.is_active:
            return
            
        self.learned_values[proposal_number] = value
        if self.consensus_value is None or proposal_number > max(self.learned_values.keys()):
            self.consensus_value = value
            
        self.logger.info(f"LEARNER: Learned value '{value}' from proposal {proposal_number}")
        self._log_event(f"LEARNER: Learned consensus value '{value}' from proposal #{proposal_number}")

    def get_status(self) -> Dict[str, Any]:
        """Obtiene el estado actual del nodo"""
        return {
            "node_id": self.node_id,
            "is_active": self.is_active,
            "promised_proposal": self.promised_proposal,
            "accepted_proposal": self.accepted_proposal,
            "accepted_value": self.accepted_value,
            "consensus_value": self.consensus_value,
            "learned_values_count": len(self.learned_values),
            "stats": {
                "prepare_requests_sent": self.prepare_requests_sent,
                "prepare_responses_received": self.prepare_responses_received,
                "accept_requests_sent": self.accept_requests_sent,
                "accept_responses_received": self.accept_responses_received,
                "proposals_successful": self.proposals_successful,
                "proposals_failed": self.proposals_failed
            }
        }

    def get_execution_log(self) -> List[str]:
        """Obtiene el log completo de ejecución"""
        return self.execution_log.copy()

    def print_execution_log(self):
        """Imprime el log de ejecución del nodo"""
        print(f"\n{'='*80}")
        print(f"EXECUTION LOG - Node {self.node_id}")
        print(f"{'='*80}")
        for entry in self.execution_log:
            print(entry)
        print(f"{'='*80}")

class PaxosCluster:
    def __init__(self, node_ids: List[str]):
        self.nodes: Dict[str, PaxosNode] = {}
        self.consensus_history: List[Tuple[str, str, float]] = []  # (proposer, value, timestamp)
        
        # Create all nodes
        for node_id in node_ids:
            self.nodes[node_id] = PaxosNode(node_id, node_ids, self)
        
        self.logger = logging.getLogger("PaxosCluster")
        time.sleep(0.5)  # Allow nodes to initialize

    def get_node(self, node_id: str) -> Optional[PaxosNode]:
        return self.nodes.get(node_id)

    def propose_value(self, proposer_id: str, value: str) -> bool:
        """Propone un valor usando un nodo específico como proposer"""
        proposer = self.get_node(proposer_id)
        if proposer:
            success = proposer.propose_value(value)
            if success:
                self.consensus_history.append((proposer_id, value, time.time()))
            return success
        return False

    def simulate_node_failure(self, node_id: str):
        """Simula el fallo de un nodo"""
        if node_id in self.nodes:
            self.nodes[node_id].simulate_failure()

    def recover_node(self, node_id: str):
        """Recupera un nodo del fallo"""
        if node_id in self.nodes:
            self.nodes[node_id].recover_from_failure()

    def get_cluster_status(self) -> Dict[str, Any]:
        """Obtiene el estado del cluster"""
        status = {}
        for node_id, node in self.nodes.items():
            status[node_id] = node.get_status()
        return status

    def get_consensus_values(self) -> Dict[str, str]:
        """Obtiene los valores de consenso de todos los nodos"""
        consensus_values = {}
        for node_id, node in self.nodes.items():
            if node.consensus_value is not None:
                consensus_values[node_id] = node.consensus_value
        return consensus_values

    def print_all_logs(self):
        """Imprime los logs de todos los nodos"""
        for node_id, node in self.nodes.items():
            node.print_execution_log()

    def save_logs_to_file(self, filename: str):
        """Guarda todos los logs en un archivo"""
        with open(filename, 'w') as f:
            f.write("PAXOS CONSENSUS ALGORITHM - EXECUTION LOGS\n")
            f.write("="*80 + "\n\n")
            
            f.write("CONSENSUS HISTORY:\n")
            for i, (proposer, value, timestamp) in enumerate(self.consensus_history):
                f.write(f"{i+1}. Proposer: {proposer}, Value: '{value}', Time: {time.ctime(timestamp)}\n")
            f.write("\n" + "="*80 + "\n\n")
            
            for node_id, node in self.nodes.items():
                f.write(f"NODE {node_id} EXECUTION LOG:\n")
                f.write("-" * 40 + "\n")
                for entry in node.get_execution_log():
                    f.write(entry + "\n")
                f.write("\n" + "="*80 + "\n\n")

def print_cluster_status(cluster: PaxosCluster):
    """Imprime el estado del cluster"""
    print("\n" + "="*100)
    print("PAXOS CLUSTER STATUS")
    print("="*100)
    
    status = cluster.get_cluster_status()
    consensus_values = cluster.get_consensus_values()
    
    for node_id, node_status in status.items():
        active_str = "ACTIVE" if node_status["is_active"] else "FAILED"
        promised = node_status["promised_proposal"]
        accepted = node_status["accepted_proposal"]
        consensus = node_status["consensus_value"]
        
        print(f"Node {node_id:>3}: {active_str:<6} | Promised: {promised:>6} | "
              f"Accepted: {accepted:>6} | Consensus: {consensus or 'None':<10}")
        
        stats = node_status["stats"]
        print(f"         Stats: Prepare(S:{stats['prepare_requests_sent']:>2}, R:{stats['prepare_responses_received']:>2}) | "
              f"Accept(S:{stats['accept_requests_sent']:>2}, R:{stats['accept_responses_received']:>2}) | "
              f"Success:{stats['proposals_successful']:>2} | Failed:{stats['proposals_failed']:>2}")
    
    print("\nConsensus Values:")
    if consensus_values:
        for node_id, value in consensus_values.items():
            print(f"  Node {node_id}: '{value}'")
    else:
        print("  No consensus achieved yet")
    
    print("="*100)

def demo_paxos_consensus():
    """Demostración del algoritmo Paxos"""
    print("🚀 DEMO: Algoritmo de Consenso Paxos")
    print("="*50)
    
    # Create cluster with 5 nodes
    node_ids = ["A", "B", "C", "D", "E"]
    cluster = PaxosCluster(node_ids)
    
    print("Cluster Paxos inicializado con 5 nodos...")
    time.sleep(1)
    
    print_cluster_status(cluster)
    
    # Propose some values
    print("\n📝 Proponiendo valores para consenso...")
    
    proposals = [
        ("A", "valor_1"),
        ("B", "valor_2"), 
        ("C", "comando_X"),
        ("A", "final_value")
    ]
    
    for i, (proposer, value) in enumerate(proposals):
        print(f"\n--- Propuesta {i+1}: Node {proposer} propone '{value}' ---")
        success = cluster.propose_value(proposer, value)
        print(f"Resultado: {'✅ EXITOSO' if success else '❌ FALLIDO'}")
        time.sleep(1)
        print_cluster_status(cluster)
    
    # Simulate node failure during consensus
    print(f"\n💥 Simulando fallo de Node C durante nueva propuesta...")
    cluster.simulate_node_failure("C")
    
    print(f"\n--- Propuesta con nodo fallido: Node A propone 'recovery_test' ---")
    success = cluster.propose_value("A", "recovery_test")
    print(f"Resultado: {'✅ EXITOSO' if success else '❌ FALLIDO'}")
    print_cluster_status(cluster)
    
    # Recover node
    print(f"\n🔄 Recuperando Node C...")
    cluster.recover_node("C")
    time.sleep(1)
    
    print(f"\n--- Propuesta final: Node C propone 'after_recovery' ---")
    success = cluster.propose_value("C", "after_recovery")
    print(f"Resultado: {'✅ EXITOSO' if success else '❌ FALLIDO'}")
    print_cluster_status(cluster)
    
    # Print execution logs
    print("\n📋 Logs detallados de ejecución:")
    cluster.print_all_logs()
    
    # Save logs to file
    log_filename = f"paxos_execution_log_{int(time.time())}.txt"
    cluster.save_logs_to_file(log_filename)
    print(f"\n💾 Logs guardados en: {log_filename}")
    
    print("\n✅ Demo Paxos completado. El sistema demostró:")
    print("   • Algoritmo de consenso Paxos (Prepare/Accept)")
    print("   • Manejo de múltiples proposers")
    print("   • Tolerancia a fallos de nodos")
    print("   • Logs detallados de todas las fases")
    print("   • Garantías de consistencia y seguridad")
    
    return cluster

if __name__ == "__main__":
    # Run the demo
    cluster = demo_paxos_consensus()
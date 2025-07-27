import random
import time
import threading
import json
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import queue
import logging

# Configurar logging para archivo y consola
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler('raft_log.txt', mode='w', encoding='utf-8')
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)

logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])

class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate" 
    LEADER = "leader"

@dataclass
class LogEntry:
    term: int
    index: int
    command: str
    
@dataclass
class VoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

@dataclass
class VoteResponse:
    term: int
    vote_granted: bool

@dataclass
class AppendEntriesRequest:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int

@dataclass
class AppendEntriesResponse:
    term: int
    success: bool

class RaftNode:
    def __init__(self, node_id: str, peers: List[str], cluster=None):
        self.node_id = node_id
        self.peers = peers
        self.cluster = cluster
        self.state = NodeState.FOLLOWER
        
        # Persistent state
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        
        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        
        # Leader state
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        # Timing
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(5, 10)  # 5-10 seconds
        self.heartbeat_interval = 2  # 2 seconds
        
        # Communication
        self.message_queue = queue.Queue()
        self.is_active = True
        self.logger = logging.getLogger(f"Node-{node_id}")
        
        # Applied commands (state machine)
        self.applied_commands: Dict[str, Any] = {}
        
        # Start background threads
        self.election_thread = threading.Thread(target=self._election_timer, daemon=True)
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_timer, daemon=True)
        self.message_thread = threading.Thread(target=self._process_messages, daemon=True)
        
        self.election_thread.start()
        self.heartbeat_thread.start()
        self.message_thread.start()
        
        self.logger.info(f"Node {node_id} initialized as FOLLOWER")

    def simulate_failure(self):
        """Simula el fallo del nodo"""
        self.is_active = False
        self.logger.warning(f"Node {self.node_id} has FAILED")

    def recover_from_failure(self):
        """Recupera el nodo del fallo"""
        self.is_active = True
        self.state = NodeState.FOLLOWER
        self.logger.info(f"Node {self.node_id} has RECOVERED")

    def _election_timer(self):
        """Timer para elecciones"""
        while True:
            time.sleep(0.5)
            if not self.is_active:
                continue
                
            if self.state != NodeState.LEADER:
                if time.time() - self.last_heartbeat > self.election_timeout:
                    self._start_election()

    def _heartbeat_timer(self):
        """Timer para heartbeats del líder"""
        while True:
            time.sleep(self.heartbeat_interval)
            if not self.is_active:
                continue
                
            if self.state == NodeState.LEADER:
                self._send_heartbeats()

    def _start_election(self):
        """Inicia una elección"""
        if not self.is_active:
            return
            
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(5, 10)
        
        self.logger.info(f"Starting election for term {self.current_term}")
        
        votes_received = 1  # Vote for self
        last_log_index = len(self.log) - 1 if self.log else -1
        last_log_term = self.log[-1].term if self.log else 0
        
        # Send vote requests to all peers
        for peer_id in self.peers:
            if peer_id != self.node_id:
                vote_request = VoteRequest(
                    term=self.current_term,
                    candidate_id=self.node_id,
                    last_log_index=last_log_index,
                    last_log_term=last_log_term
                )
                # Simulate sending vote request
                response = self._simulate_vote_request(peer_id, vote_request)
                if response and response.vote_granted:
                    votes_received += 1
                    
        # Check if won election
        if votes_received > len(self.peers) // 2:
            self._become_leader()
        else:
            self.state = NodeState.FOLLOWER
            self.logger.info(f"Election failed, reverting to FOLLOWER")

    def _become_leader(self):
        """Se convierte en líder"""
        self.state = NodeState.LEADER
        self.logger.info(f"Became LEADER for term {self.current_term}")
        
        # Initialize leader state
        for peer_id in self.peers:
            if peer_id != self.node_id:
                self.next_index[peer_id] = len(self.log)
                self.match_index[peer_id] = -1
                
        # Send initial heartbeat
        self._send_heartbeats()

    def _send_heartbeats(self):
        """Envía heartbeats a todos los seguidores"""
        if not self.is_active or self.state != NodeState.LEADER:
            return
            
        for peer_id in self.peers:
            if peer_id != self.node_id:
                prev_log_index = self.next_index.get(peer_id, 0) - 1
                prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 and prev_log_index < len(self.log) else 0
                
                request = AppendEntriesRequest(
                    term=self.current_term,
                    leader_id=self.node_id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=[],  # Heartbeat has no entries
                    leader_commit=self.commit_index
                )
                self._simulate_append_entries(peer_id, request)

    def propose_command(self, command: str) -> bool:
        """Propone un comando para consenso"""
        if not self.is_active or self.state != NodeState.LEADER:
            self.logger.warning(f"Cannot propose command: not leader")
            return False
            
        # Add entry to log
        log_entry = LogEntry(
            term=self.current_term,
            index=len(self.log),
            command=command
        )
        self.log.append(log_entry)
        self.logger.info(f"Proposed command: {command} at index {log_entry.index}")
        
        # Replicate to followers
        return self._replicate_log_entry(log_entry)

    def _replicate_log_entry(self, entry: LogEntry) -> bool:
        """Replica una entrada del log a los seguidores"""
        if not self.is_active or self.state != NodeState.LEADER:
            return False
            
        success_count = 1  # Leader counts as success
        
        for peer_id in self.peers:
            if peer_id != self.node_id:
                prev_log_index = entry.index - 1
                prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0
                
                request = AppendEntriesRequest(
                    term=self.current_term,
                    leader_id=self.node_id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=[entry],
                    leader_commit=self.commit_index
                )
                
                response = self._simulate_append_entries(peer_id, request)
                if response and response.success:
                    success_count += 1
                    
        # Check if majority accepted
        if success_count > len(self.peers) // 2:
            self.commit_index = entry.index
            self._apply_committed_entries()
            self.logger.info(f"Command committed: {entry.command}")
            return True
        else:
            self.logger.warning(f"Failed to replicate command: {entry.command}")
            return False

    def _apply_committed_entries(self):
        """Aplica las entradas comprometidas al estado de la máquina"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            if self.last_applied < len(self.log):
                entry = self.log[self.last_applied]
                # Parse command (assume format "key=value")
                if "=" in entry.command:
                    key, value = entry.command.split("=", 1)
                    self.applied_commands[key.strip()] = value.strip()
                    self.logger.info(f"Applied command: {key}={value}")

    def _simulate_vote_request(self, peer_id: str, request: VoteRequest) -> Optional[VoteResponse]:
        """Simula el envío de una solicitud de voto"""
        # In a real implementation, this would send over network
        if self.cluster:
            peer = self.cluster.get_node(peer_id)
            if peer and peer.is_active:
                return peer._handle_vote_request(request)
        return None

    def _simulate_append_entries(self, peer_id: str, request: AppendEntriesRequest) -> Optional[AppendEntriesResponse]:
        """Simula el envío de AppendEntries"""
        if self.cluster:
            peer = self.cluster.get_node(peer_id)
            if peer and peer.is_active:
                return peer._handle_append_entries(request)
        return None

    def _handle_vote_request(self, request: VoteRequest) -> VoteResponse:
        """Maneja una solicitud de voto"""
        if not self.is_active:
            return VoteResponse(term=self.current_term, vote_granted=False)
            
        # Update term if necessary
        if request.term > self.current_term:
            self.current_term = request.term
            self.voted_for = None
            self.state = NodeState.FOLLOWER
            
        vote_granted = False
        
        if (request.term == self.current_term and 
            (self.voted_for is None or self.voted_for == request.candidate_id)):
            
            # Check if candidate's log is up-to-date
            last_log_index = len(self.log) - 1 if self.log else -1
            last_log_term = self.log[-1].term if self.log else 0
            
            log_up_to_date = (request.last_log_term > last_log_term or
                            (request.last_log_term == last_log_term and 
                             request.last_log_index >= last_log_index))
            
            if log_up_to_date:
                vote_granted = True
                self.voted_for = request.candidate_id
                self.last_heartbeat = time.time()
                
        self.logger.info(f"Vote request from {request.candidate_id}: {'GRANTED' if vote_granted else 'DENIED'}")
        return VoteResponse(term=self.current_term, vote_granted=vote_granted)

    def _handle_append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        """Maneja una solicitud AppendEntries"""
        if not self.is_active:
            return AppendEntriesResponse(term=self.current_term, success=False)
            
        self.last_heartbeat = time.time()
        
        # Update term if necessary
        if request.term > self.current_term:
            self.current_term = request.term
            self.voted_for = None
            
        if request.term == self.current_term:
            self.state = NodeState.FOLLOWER
            
        success = False
        
        if request.term == self.current_term:
            # Check if log matches
            if (request.prev_log_index == -1 or
                (request.prev_log_index < len(self.log) and
                 self.log[request.prev_log_index].term == request.prev_log_term)):
                
                success = True
                
                # Append new entries
                if request.entries:
                    # Remove conflicting entries
                    if request.prev_log_index + 1 < len(self.log):
                        self.log = self.log[:request.prev_log_index + 1]
                    
                    # Append new entries
                    self.log.extend(request.entries)
                    self.logger.info(f"Appended {len(request.entries)} entries")
                
                # Update commit index
                if request.leader_commit > self.commit_index:
                    self.commit_index = min(request.leader_commit, len(self.log) - 1)
                    self._apply_committed_entries()
                    
        return AppendEntriesResponse(term=self.current_term, success=success)

    def _process_messages(self):
        """Procesa mensajes en cola"""
        while True:
            try:
                message = self.message_queue.get(timeout=1)
                # Process message here if needed
            except queue.Empty:
                continue

    def get_status(self) -> Dict[str, Any]:
        """Obtiene el estado actual del nodo"""
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "current_term": self.current_term,
            "is_active": self.is_active,
            "log_length": len(self.log),
            "commit_index": self.commit_index,
            "applied_commands": self.applied_commands.copy(),
            "voted_for": self.voted_for
        }

class RaftCluster:
    def __init__(self, node_ids: List[str]):
        self.nodes: Dict[str, RaftNode] = {}
        
        # Create all nodes
        for node_id in node_ids:
            self.nodes[node_id] = RaftNode(node_id, node_ids, self)
            
        time.sleep(1)  # Allow nodes to initialize
        
    def get_node(self, node_id: str) -> Optional[RaftNode]:
        return self.nodes.get(node_id)
        
    def get_leader(self) -> Optional[RaftNode]:
        """Encuentra el líder actual"""
        for node in self.nodes.values():
            if node.is_active and node.state == NodeState.LEADER:
                return node
        return None
        
    def propose_command(self, command: str) -> bool:
        """Propone un comando al cluster"""
        leader = self.get_leader()
        if leader:
            return leader.propose_command(command)
        else:
            print("No leader available")
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

def print_cluster_status(cluster: RaftCluster):
    """Imprime el estado del cluster"""
    print("\n" + "="*80)
    print("CLUSTER STATUS")
    print("="*80)
    
    status = cluster.get_cluster_status()
    
    for node_id, node_status in status.items():
        state_str = node_status["state"].upper()
        active_str = "ACTIVE" if node_status["is_active"] else "FAILED"
        
        print(f"Node {node_id:>3}: {state_str:<9} | Term: {node_status['current_term']:>2} | "
              f"Status: {active_str:<6} | Log: {node_status['log_length']:>2} entries | "
              f"Committed: {node_status['commit_index']:>2}")
        
        if node_status["applied_commands"]:
            print(f"         Applied commands: {node_status['applied_commands']}")
    
    leader = cluster.get_leader()
    if leader:
        print(f"\nCurrent Leader: Node {leader.node_id}")
    else:
        print("\nNo current leader")
    
    print("="*80)

def demo_raft_consensus():
    """Demostración del algoritmo Raft"""
    print("🚀 DEMO: Algoritmo de Consenso Raft")
    print("="*50)
    
    # Create cluster with 5 nodes
    node_ids = ["A", "B", "C", "D", "E"]
    cluster = RaftCluster(node_ids)
    
    print("Cluster inicializado con 5 nodos...")
    time.sleep(3)  # Wait for leader election
    
    print_cluster_status(cluster)
    
    # Wait a bit more to ensure leader election
    print("\n⏳ Esperando elección de líder...")
    time.sleep(5)
    print_cluster_status(cluster)
    
    # Propose some commands
    print("\n📝 Proponiendo comandos para consenso...")
    commands = ["A=1", "B=2", "C=hello", "D=world"]
    
    for i, cmd in enumerate(commands):
        print(f"\nProponiendo comando {i+1}: {cmd}")
        success = cluster.propose_command(cmd)
        print(f"Resultado: {'✅ EXITOSO' if success else '❌ FALLIDO'}")
        time.sleep(2)
        
    print_cluster_status(cluster)
    
    # Simulate leader failure
    leader = cluster.get_leader()
    if leader:
        print(f"\n💥 Simulando fallo del líder: Node {leader.node_id}")
        cluster.simulate_node_failure(leader.node_id)
        
        print("\n⏳ Esperando nueva elección de líder...")
        time.sleep(8)  # Wait for new leader election
        print_cluster_status(cluster)
        
        # Try to propose another command
        print("\n📝 Proponiendo comando después del fallo del líder...")
        success = cluster.propose_command("E=recovery_test")
        print(f"Resultado: {'✅ EXITOSO' if success else '❌ FALLIDO'}")
        time.sleep(2)
        print_cluster_status(cluster)
        
        # Recover the failed node
        print(f"\n🔄 Recuperando Node {leader.node_id}...")
        cluster.recover_node(leader.node_id)
        time.sleep(3)
        print_cluster_status(cluster)
    
    print("\n✅ Demo completado. El sistema demostró:")
    print("   • Elección automática de líder")
    print("   • Replicación de comandos")
    print("   • Tolerancia a fallos")
    print("   • Recuperación automática")

if __name__ == "__main__":
    # Create global cluster reference for simulation
    cluster = None
    demo_raft_consensus()
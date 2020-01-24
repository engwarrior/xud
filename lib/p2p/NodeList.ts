import { EventEmitter } from 'events';
import { ReputationEvent } from '../constants/enums';
import { NodeFactory, NodeInstance, ReputationEventInstance } from '../db/types';
import addressUtils from '../utils/addressUtils';
import errors from './errors';
import P2PRepository from './P2PRepository';
import { Address, NodeConnectionInfo } from './types';

type NodeReputation = {
  reputationScore: number;
  banned?: boolean;
};

export const reputationEventWeight = {
  [ReputationEvent.ManualBan]: Number.NEGATIVE_INFINITY,
  [ReputationEvent.ManualUnban]: 0,
  [ReputationEvent.PacketTimeout]: -1,
  [ReputationEvent.SwapFailure]: -10,
  [ReputationEvent.SwapSuccess]: 1,
  [ReputationEvent.WireProtocolErr]: -5,
  [ReputationEvent.InvalidAuth]: -20,
  [ReputationEvent.SwapTimeout]: -15,
  [ReputationEvent.SwapMisbehavior]: -20,
};

// TODO: remove reputation events after certain amount of time

interface NodeList {
  on(event: 'node.ban', listener: (nodePubKey: string, events: ReputationEventInstance[]) => void): this;
  on(event: 'node.unban', listener: (nodePubKey: string) => void): this;
  emit(event: 'node.ban', nodePubKey: string, events: ReputationEventInstance[]): boolean;
  emit(event: 'node.unban', nodePubKey: string): boolean;
}

/** A wrapper class with help methods that represents a list of known nodes. */
class NodeList extends EventEmitter {
  /** A map of all known node pub keys to their instance in the database.  */
  private nodes = new Map<string, NodeInstance>();

  private static readonly BAN_THRESHOLD = -50;

  public get count() {
    return this.nodes.size;
  }

  constructor(private repository: P2PRepository) {
    super();
  }

  /**
   * Checks if we are banned by this node
   */
  public isBannedBy = (nodePubKey: string): boolean => {
    const node = this.nodes.get(nodePubKey);
    return node ? node.bannedBy : false;
  }

  /**
   * Persists whether a node has banned us.
   */
  public setBannedBy = async (nodePubKey: string, bannedBy: boolean) => {
    const node = this.nodes.get(nodePubKey);
    if (!node) {
      throw errors.NODE_UNKNOWN(nodePubKey);
    }
    node.bannedBy = bannedBy;
    await this.save(node);
  }

  /**
   * Checks if a node with a given nodePubKey is known to us.
   */
  public has = (nodePubKey: string): boolean => {
    return this.nodes.has(nodePubKey);
  }

  public forEach = (callback: (node: NodeInstance) => void) => {
    this.nodes.forEach(callback);
  }

  /**
   * Bans a node by nodePubKey.
   */
  public ban = async (nodePubKey: string) => {
    await this.addReputationEvent(nodePubKey, ReputationEvent.ManualBan);
  }

  /**
   * Unbans a node by nodePubKey.
   */
  public unBan = async (nodePubKey: string) => {
    await this.addReputationEvent(nodePubKey, ReputationEvent.ManualUnban);
  }

  public isBanned = (nodePubKey: string): boolean => {
    const node = this.nodes.get(nodePubKey);
    return node ? node.banned : false;
  }

  /**
   * Gets a node's reputation score and whether it is banned
   * @param nodePubKey The node pub key of the node for which to get reputation information
   */
  public getNodeReputation = (nodePubKey: string): NodeReputation => {
    const node = this.nodes.get(nodePubKey);
    if (node) {
      const { reputationScore, banned } = node;
      return {
        reputationScore,
        banned,
      };
    } else {
      throw errors.NODE_UNKNOWN(nodePubKey);
    }
  }

  public getNodeConnectionInfo = (nodePubKey: string): NodeConnectionInfo => {
    const node = this.nodes.get(nodePubKey);
    if (node) {
      return {
        nodePubKey,
        addresses: node.addresses,
        lastAddress: node.lastAddress,
      };
    } else {
      throw errors.NODE_UNKNOWN(nodePubKey);
    }
  }

  /**
   * Load this NodeList from the database.
   */
  public load = async (): Promise<void> => {
    const nodes = await this.repository.getNodes();

    const reputationLoadPromises: Promise<void>[] = [];
    nodes.forEach((node) => {
      this.nodes.set(node.nodePubKey, node);
      const reputationLoadPromise = this.repository.getReputationEvents(node).then((events) => {
        events.forEach(({ event }) => {
          this.updateReputationScore(node, event);
        });
      });
      reputationLoadPromises.push(reputationLoadPromise);
    });
    await Promise.all(reputationLoadPromises);
  }

  /**
   * Persists a node to the database and adds it to the node list.
   */
  public createNode = async (nodeFactory: NodeFactory) => {
    const node = await this.repository.addNodeIfNotExists(nodeFactory);
    if (node) {
      this.nodes.set(node.nodePubKey, node);
    }
  }

  /**
   * Update a node's addresses.
   * @return true if the specified node exists and was updated, false otherwise
   */
  public updateAddresses = async (nodePubKey: string, addresses: Address[] = [], lastAddress?: Address): Promise<boolean> => {
    const node = this.nodes.get(nodePubKey);
    if (node) {
      // avoid overriding the `lastConnected` field for existing matching addresses unless a new value was set
      node.addresses = addresses.map((newAddress) => {
        const oldAddress = node.addresses.find(address => addressUtils.areEqual(address, newAddress));
        if (oldAddress && !newAddress.lastConnected) {
          return oldAddress;
        } else {
          return newAddress;
        }
      });

      if (lastAddress) {
        node.lastAddress = lastAddress;
      }

      await this.save(node);
      return true;
    }

    return false;
  }

  /**
   * Add a reputation event to the node's history
   */
  public addReputationEvent = async (nodePubKey: string, event: ReputationEvent) => {
    const node = this.nodes.get(nodePubKey);

    if (node) {
      const addReputationEventPromise = this.repository.addReputationEvent({ event, nodeId: node.id });

      this.updateReputationScore(node, event);

      if (node.reputationScore < NodeList.BAN_THRESHOLD) {
        await this.setBanned(node, true);

        const events = await this.repository.getReputationEvents(node);
        this.emit('node.ban', nodePubKey, events);
      } else if (node.banned) {
        // If the reputationScore is not below the banThreshold but node.banned
        // is true that means that the node was unbanned
        await this.setBanned(node, false);
        this.emit('node.unban', nodePubKey);
      }

      await addReputationEventPromise;
    } else {
      throw errors.NODE_UNKNOWN(nodePubKey);
    }
  }

  public removeAddress = async (nodePubKey: string, address: Address) => {
    const node = this.nodes.get(nodePubKey);
    if (node) {
      const index = node.addresses.findIndex(existingAddress => addressUtils.areEqual(address, existingAddress));
      if (index > -1) {
        node.addresses = [...node.addresses.slice(0, index), ...node.addresses.slice(index + 1)];
        await this.save(node);
        return true;
      }

      // if the lastAddress is removed, then re-assigning lastAddress with the latest connected advertised address
      if (node.lastAddress && addressUtils.areEqual(address, node.lastAddress)) {
        node.lastAddress = addressUtils.sortByLastConnected(node.addresses)[0];
      }
    }

    return false;
  }

  private save = async (node: NodeInstance) => {
    await node.save();
    this.nodes.set(node.nodePubKey, node);
  }

  private updateReputationScore = (node: NodeInstance, event: ReputationEvent) => {
    switch (event) {
      case (ReputationEvent.ManualBan):
      case (ReputationEvent.ManualUnban): {
        node.reputationScore = reputationEventWeight[event];
        break;
      }
      default: node.reputationScore += reputationEventWeight[event]; break;
    }
  }

  private setBanned = async (node: NodeInstance, banned: boolean) => {
    node.banned = banned;
    await this.save(node);
  }
}

export default NodeList;

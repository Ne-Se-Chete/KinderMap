import { query } from "sdk/db";
import { producer } from "sdk/messaging";
import { extensions } from "sdk/extensions";
import { dao as daoApi } from "sdk/db";

export interface KinderGardenEntity {
    readonly Id: number;
    Name?: string;
    Address?: number;
    Capacity?: number;
    Kindergartners?: number;
    Property6?: string;
}

export interface KinderGardenCreateEntity {
    readonly Name?: string;
    readonly Address?: number;
    readonly Capacity?: number;
    readonly Kindergartners?: number;
    readonly Property6?: string;
}

export interface KinderGardenUpdateEntity extends KinderGardenCreateEntity {
    readonly Id: number;
}

export interface KinderGardenEntityOptions {
    $filter?: {
        equals?: {
            Id?: number | number[];
            Name?: string | string[];
            Address?: number | number[];
            Capacity?: number | number[];
            Kindergartners?: number | number[];
            Property6?: string | string[];
        };
        notEquals?: {
            Id?: number | number[];
            Name?: string | string[];
            Address?: number | number[];
            Capacity?: number | number[];
            Kindergartners?: number | number[];
            Property6?: string | string[];
        };
        contains?: {
            Id?: number;
            Name?: string;
            Address?: number;
            Capacity?: number;
            Kindergartners?: number;
            Property6?: string;
        };
        greaterThan?: {
            Id?: number;
            Name?: string;
            Address?: number;
            Capacity?: number;
            Kindergartners?: number;
            Property6?: string;
        };
        greaterThanOrEqual?: {
            Id?: number;
            Name?: string;
            Address?: number;
            Capacity?: number;
            Kindergartners?: number;
            Property6?: string;
        };
        lessThan?: {
            Id?: number;
            Name?: string;
            Address?: number;
            Capacity?: number;
            Kindergartners?: number;
            Property6?: string;
        };
        lessThanOrEqual?: {
            Id?: number;
            Name?: string;
            Address?: number;
            Capacity?: number;
            Kindergartners?: number;
            Property6?: string;
        };
    },
    $select?: (keyof KinderGardenEntity)[],
    $sort?: string | (keyof KinderGardenEntity)[],
    $order?: 'asc' | 'desc',
    $offset?: number,
    $limit?: number,
}

interface KinderGardenEntityEvent {
    readonly operation: 'create' | 'update' | 'delete';
    readonly table: string;
    readonly entity: Partial<KinderGardenEntity>;
    readonly key: {
        name: string;
        column: string;
        value: number;
    }
}

interface KinderGardenUpdateEntityEvent extends KinderGardenEntityEvent {
    readonly previousEntity: KinderGardenEntity;
}

export class KinderGardenRepository {

    private static readonly DEFINITION = {
        table: "KINDERGARDENS",
        properties: [
            {
                name: "Id",
                column: "KINDERGARDENS_ID",
                type: "INTEGER",
                id: true,
                autoIncrement: true,
            },
            {
                name: "Name",
                column: "KINDERGARDENS_NAME",
                type: "VARCHAR",
            },
            {
                name: "Address",
                column: "KINDERGARDENS_ADDRESS",
                type: "INTEGER",
            },
            {
                name: "Capacity",
                column: "KINDERGARDENS_CAPACITY",
                type: "INTEGER",
            },
            {
                name: "Kindergartners",
                column: "KINDERGARDENS_KINDERGARTNERS",
                type: "INTEGER",
            },
            {
                name: "Property6",
                column: "KINDERGARDENS_PROPERTY6",
                type: "VARCHAR",
            }
        ]
    };

    private readonly dao;

    constructor(dataSource = "DefaultDB") {
        this.dao = daoApi.create(KinderGardenRepository.DEFINITION, null, dataSource);
    }

    public findAll(options?: KinderGardenEntityOptions): KinderGardenEntity[] {
        return this.dao.list(options);
    }

    public findById(id: number): KinderGardenEntity | undefined {
        const entity = this.dao.find(id);
        return entity ?? undefined;
    }

    public create(entity: KinderGardenCreateEntity): number {
        const id = this.dao.insert(entity);
        this.triggerEvent({
            operation: "create",
            table: "KINDERGARDENS",
            entity: entity,
            key: {
                name: "Id",
                column: "KINDERGARDENS_ID",
                value: id
            }
        });
        return id;
    }

    public update(entity: KinderGardenUpdateEntity): void {
        const previousEntity = this.findById(entity.Id);
        this.dao.update(entity);
        this.triggerEvent({
            operation: "update",
            table: "KINDERGARDENS",
            entity: entity,
            previousEntity: previousEntity,
            key: {
                name: "Id",
                column: "KINDERGARDENS_ID",
                value: entity.Id
            }
        });
    }

    public upsert(entity: KinderGardenCreateEntity | KinderGardenUpdateEntity): number {
        const id = (entity as KinderGardenUpdateEntity).Id;
        if (!id) {
            return this.create(entity);
        }

        const existingEntity = this.findById(id);
        if (existingEntity) {
            this.update(entity as KinderGardenUpdateEntity);
            return id;
        } else {
            return this.create(entity);
        }
    }

    public deleteById(id: number): void {
        const entity = this.dao.find(id);
        this.dao.remove(id);
        this.triggerEvent({
            operation: "delete",
            table: "KINDERGARDENS",
            entity: entity,
            key: {
                name: "Id",
                column: "KINDERGARDENS_ID",
                value: id
            }
        });
    }

    public count(options?: KinderGardenEntityOptions): number {
        return this.dao.count(options);
    }

    public customDataCount(): number {
        const resultSet = query.execute('SELECT COUNT(*) AS COUNT FROM "KINDERGARDENS"');
        if (resultSet !== null && resultSet[0] !== null) {
            if (resultSet[0].COUNT !== undefined && resultSet[0].COUNT !== null) {
                return resultSet[0].COUNT;
            } else if (resultSet[0].count !== undefined && resultSet[0].count !== null) {
                return resultSet[0].count;
            }
        }
        return 0;
    }

    private async triggerEvent(data: KinderGardenEntityEvent | KinderGardenUpdateEntityEvent) {
        const triggerExtensions = await extensions.loadExtensionModules("kinder-map-entities-KinderGarden", ["trigger"]);
        triggerExtensions.forEach(triggerExtension => {
            try {
                triggerExtension.trigger(data);
            } catch (error) {
                console.error(error);
            }            
        });
        producer.topic("kinder-map-entities-KinderGarden").send(JSON.stringify(data));
    }
}

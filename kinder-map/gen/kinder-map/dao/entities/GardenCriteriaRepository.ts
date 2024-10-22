import { query } from "sdk/db";
import { producer } from "sdk/messaging";
import { extensions } from "sdk/extensions";
import { dao as daoApi } from "sdk/db";

export interface GardenCriteriaEntity {
    readonly Id: number;
    Criteria?: number;
    KinderGarden?: number;
}

export interface GardenCriteriaCreateEntity {
    readonly Criteria?: number;
    readonly KinderGarden?: number;
}

export interface GardenCriteriaUpdateEntity extends GardenCriteriaCreateEntity {
    readonly Id: number;
}

export interface GardenCriteriaEntityOptions {
    $filter?: {
        equals?: {
            Id?: number | number[];
            Criteria?: number | number[];
            KinderGarden?: number | number[];
        };
        notEquals?: {
            Id?: number | number[];
            Criteria?: number | number[];
            KinderGarden?: number | number[];
        };
        contains?: {
            Id?: number;
            Criteria?: number;
            KinderGarden?: number;
        };
        greaterThan?: {
            Id?: number;
            Criteria?: number;
            KinderGarden?: number;
        };
        greaterThanOrEqual?: {
            Id?: number;
            Criteria?: number;
            KinderGarden?: number;
        };
        lessThan?: {
            Id?: number;
            Criteria?: number;
            KinderGarden?: number;
        };
        lessThanOrEqual?: {
            Id?: number;
            Criteria?: number;
            KinderGarden?: number;
        };
    },
    $select?: (keyof GardenCriteriaEntity)[],
    $sort?: string | (keyof GardenCriteriaEntity)[],
    $order?: 'asc' | 'desc',
    $offset?: number,
    $limit?: number,
}

interface GardenCriteriaEntityEvent {
    readonly operation: 'create' | 'update' | 'delete';
    readonly table: string;
    readonly entity: Partial<GardenCriteriaEntity>;
    readonly key: {
        name: string;
        column: string;
        value: number;
    }
}

interface GardenCriteriaUpdateEntityEvent extends GardenCriteriaEntityEvent {
    readonly previousEntity: GardenCriteriaEntity;
}

export class GardenCriteriaRepository {

    private static readonly DEFINITION = {
        table: "GARDENCRITERIA",
        properties: [
            {
                name: "Id",
                column: "GARDENCRITERIA_ID",
                type: "INTEGER",
                id: true,
                autoIncrement: true,
            },
            {
                name: "Criteria",
                column: "GARDENCRITERIA_CRITERIA",
                type: "INTEGER",
            },
            {
                name: "KinderGarden",
                column: "GARDENCRITERIA_KINDERGARDEN",
                type: "INTEGER",
            }
        ]
    };

    private readonly dao;

    constructor(dataSource = "DefaultDB") {
        this.dao = daoApi.create(GardenCriteriaRepository.DEFINITION, null, dataSource);
    }

    public findAll(options?: GardenCriteriaEntityOptions): GardenCriteriaEntity[] {
        return this.dao.list(options);
    }

    public findById(id: number): GardenCriteriaEntity | undefined {
        const entity = this.dao.find(id);
        return entity ?? undefined;
    }

    public create(entity: GardenCriteriaCreateEntity): number {
        const id = this.dao.insert(entity);
        this.triggerEvent({
            operation: "create",
            table: "GARDENCRITERIA",
            entity: entity,
            key: {
                name: "Id",
                column: "GARDENCRITERIA_ID",
                value: id
            }
        });
        return id;
    }

    public update(entity: GardenCriteriaUpdateEntity): void {
        const previousEntity = this.findById(entity.Id);
        this.dao.update(entity);
        this.triggerEvent({
            operation: "update",
            table: "GARDENCRITERIA",
            entity: entity,
            previousEntity: previousEntity,
            key: {
                name: "Id",
                column: "GARDENCRITERIA_ID",
                value: entity.Id
            }
        });
    }

    public upsert(entity: GardenCriteriaCreateEntity | GardenCriteriaUpdateEntity): number {
        const id = (entity as GardenCriteriaUpdateEntity).Id;
        if (!id) {
            return this.create(entity);
        }

        const existingEntity = this.findById(id);
        if (existingEntity) {
            this.update(entity as GardenCriteriaUpdateEntity);
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
            table: "GARDENCRITERIA",
            entity: entity,
            key: {
                name: "Id",
                column: "GARDENCRITERIA_ID",
                value: id
            }
        });
    }

    public count(options?: GardenCriteriaEntityOptions): number {
        return this.dao.count(options);
    }

    public customDataCount(): number {
        const resultSet = query.execute('SELECT COUNT(*) AS COUNT FROM "GARDENCRITERIA"');
        if (resultSet !== null && resultSet[0] !== null) {
            if (resultSet[0].COUNT !== undefined && resultSet[0].COUNT !== null) {
                return resultSet[0].COUNT;
            } else if (resultSet[0].count !== undefined && resultSet[0].count !== null) {
                return resultSet[0].count;
            }
        }
        return 0;
    }

    private async triggerEvent(data: GardenCriteriaEntityEvent | GardenCriteriaUpdateEntityEvent) {
        const triggerExtensions = await extensions.loadExtensionModules("kinder-map-entities-GardenCriteria", ["trigger"]);
        triggerExtensions.forEach(triggerExtension => {
            try {
                triggerExtension.trigger(data);
            } catch (error) {
                console.error(error);
            }            
        });
        producer.topic("kinder-map-entities-GardenCriteria").send(JSON.stringify(data));
    }
}

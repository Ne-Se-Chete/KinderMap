import { query } from "sdk/db";
import { producer } from "sdk/messaging";
import { extensions } from "sdk/extensions";
import { dao as daoApi } from "sdk/db";

export interface CriteriaEntity {
    readonly Id: number;
    Name?: string;
    Discription?: string;
    Points?: number;
}

export interface CriteriaCreateEntity {
    readonly Name?: string;
    readonly Discription?: string;
    readonly Points?: number;
}

export interface CriteriaUpdateEntity extends CriteriaCreateEntity {
    readonly Id: number;
}

export interface CriteriaEntityOptions {
    $filter?: {
        equals?: {
            Id?: number | number[];
            Name?: string | string[];
            Discription?: string | string[];
            Points?: number | number[];
        };
        notEquals?: {
            Id?: number | number[];
            Name?: string | string[];
            Discription?: string | string[];
            Points?: number | number[];
        };
        contains?: {
            Id?: number;
            Name?: string;
            Discription?: string;
            Points?: number;
        };
        greaterThan?: {
            Id?: number;
            Name?: string;
            Discription?: string;
            Points?: number;
        };
        greaterThanOrEqual?: {
            Id?: number;
            Name?: string;
            Discription?: string;
            Points?: number;
        };
        lessThan?: {
            Id?: number;
            Name?: string;
            Discription?: string;
            Points?: number;
        };
        lessThanOrEqual?: {
            Id?: number;
            Name?: string;
            Discription?: string;
            Points?: number;
        };
    },
    $select?: (keyof CriteriaEntity)[],
    $sort?: string | (keyof CriteriaEntity)[],
    $order?: 'asc' | 'desc',
    $offset?: number,
    $limit?: number,
}

interface CriteriaEntityEvent {
    readonly operation: 'create' | 'update' | 'delete';
    readonly table: string;
    readonly entity: Partial<CriteriaEntity>;
    readonly key: {
        name: string;
        column: string;
        value: number;
    }
}

interface CriteriaUpdateEntityEvent extends CriteriaEntityEvent {
    readonly previousEntity: CriteriaEntity;
}

export class CriteriaRepository {

    private static readonly DEFINITION = {
        table: "CRITERIA",
        properties: [
            {
                name: "Id",
                column: "CRITERIA_ID",
                type: "INTEGER",
                id: true,
                autoIncrement: true,
            },
            {
                name: "Name",
                column: "CRITERIA_NAME",
                type: "VARCHAR",
            },
            {
                name: "Discription",
                column: "CRITERIA_DISCRIPTION",
                type: "VARCHAR",
            },
            {
                name: "Points",
                column: "CRITERIA_POINTS",
                type: "INTEGER",
            }
        ]
    };

    private readonly dao;

    constructor(dataSource = "DefaultDB") {
        this.dao = daoApi.create(CriteriaRepository.DEFINITION, null, dataSource);
    }

    public findAll(options?: CriteriaEntityOptions): CriteriaEntity[] {
        return this.dao.list(options);
    }

    public findById(id: number): CriteriaEntity | undefined {
        const entity = this.dao.find(id);
        return entity ?? undefined;
    }

    public create(entity: CriteriaCreateEntity): number {
        const id = this.dao.insert(entity);
        this.triggerEvent({
            operation: "create",
            table: "CRITERIA",
            entity: entity,
            key: {
                name: "Id",
                column: "CRITERIA_ID",
                value: id
            }
        });
        return id;
    }

    public update(entity: CriteriaUpdateEntity): void {
        const previousEntity = this.findById(entity.Id);
        this.dao.update(entity);
        this.triggerEvent({
            operation: "update",
            table: "CRITERIA",
            entity: entity,
            previousEntity: previousEntity,
            key: {
                name: "Id",
                column: "CRITERIA_ID",
                value: entity.Id
            }
        });
    }

    public upsert(entity: CriteriaCreateEntity | CriteriaUpdateEntity): number {
        const id = (entity as CriteriaUpdateEntity).Id;
        if (!id) {
            return this.create(entity);
        }

        const existingEntity = this.findById(id);
        if (existingEntity) {
            this.update(entity as CriteriaUpdateEntity);
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
            table: "CRITERIA",
            entity: entity,
            key: {
                name: "Id",
                column: "CRITERIA_ID",
                value: id
            }
        });
    }

    public count(options?: CriteriaEntityOptions): number {
        return this.dao.count(options);
    }

    public customDataCount(): number {
        const resultSet = query.execute('SELECT COUNT(*) AS COUNT FROM "CRITERIA"');
        if (resultSet !== null && resultSet[0] !== null) {
            if (resultSet[0].COUNT !== undefined && resultSet[0].COUNT !== null) {
                return resultSet[0].COUNT;
            } else if (resultSet[0].count !== undefined && resultSet[0].count !== null) {
                return resultSet[0].count;
            }
        }
        return 0;
    }

    private async triggerEvent(data: CriteriaEntityEvent | CriteriaUpdateEntityEvent) {
        const triggerExtensions = await extensions.loadExtensionModules("kinder-map-entities-Criteria", ["trigger"]);
        triggerExtensions.forEach(triggerExtension => {
            try {
                triggerExtension.trigger(data);
            } catch (error) {
                console.error(error);
            }            
        });
        producer.topic("kinder-map-entities-Criteria").send(JSON.stringify(data));
    }
}

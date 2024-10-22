import { query } from "sdk/db";
import { producer } from "sdk/messaging";
import { extensions } from "sdk/extensions";
import { dao as daoApi } from "sdk/db";

export interface StreetEntity {
    readonly Id: number;
    Name?: string;
    Neighborhood?: number;
}

export interface StreetCreateEntity {
    readonly Name?: string;
    readonly Neighborhood?: number;
}

export interface StreetUpdateEntity extends StreetCreateEntity {
    readonly Id: number;
}

export interface StreetEntityOptions {
    $filter?: {
        equals?: {
            Id?: number | number[];
            Name?: string | string[];
            Neighborhood?: number | number[];
        };
        notEquals?: {
            Id?: number | number[];
            Name?: string | string[];
            Neighborhood?: number | number[];
        };
        contains?: {
            Id?: number;
            Name?: string;
            Neighborhood?: number;
        };
        greaterThan?: {
            Id?: number;
            Name?: string;
            Neighborhood?: number;
        };
        greaterThanOrEqual?: {
            Id?: number;
            Name?: string;
            Neighborhood?: number;
        };
        lessThan?: {
            Id?: number;
            Name?: string;
            Neighborhood?: number;
        };
        lessThanOrEqual?: {
            Id?: number;
            Name?: string;
            Neighborhood?: number;
        };
    },
    $select?: (keyof StreetEntity)[],
    $sort?: string | (keyof StreetEntity)[],
    $order?: 'asc' | 'desc',
    $offset?: number,
    $limit?: number,
}

interface StreetEntityEvent {
    readonly operation: 'create' | 'update' | 'delete';
    readonly table: string;
    readonly entity: Partial<StreetEntity>;
    readonly key: {
        name: string;
        column: string;
        value: number;
    }
}

interface StreetUpdateEntityEvent extends StreetEntityEvent {
    readonly previousEntity: StreetEntity;
}

export class StreetRepository {

    private static readonly DEFINITION = {
        table: "STREET",
        properties: [
            {
                name: "Id",
                column: "STREET_ID",
                type: "INTEGER",
                id: true,
                autoIncrement: true,
            },
            {
                name: "Name",
                column: "STREET_NAME",
                type: "VARCHAR",
            },
            {
                name: "Neighborhood",
                column: "STREET_NEIGHBORHOOD",
                type: "INTEGER",
            }
        ]
    };

    private readonly dao;

    constructor(dataSource = "DefaultDB") {
        this.dao = daoApi.create(StreetRepository.DEFINITION, null, dataSource);
    }

    public findAll(options?: StreetEntityOptions): StreetEntity[] {
        return this.dao.list(options);
    }

    public findById(id: number): StreetEntity | undefined {
        const entity = this.dao.find(id);
        return entity ?? undefined;
    }

    public create(entity: StreetCreateEntity): number {
        const id = this.dao.insert(entity);
        this.triggerEvent({
            operation: "create",
            table: "STREET",
            entity: entity,
            key: {
                name: "Id",
                column: "STREET_ID",
                value: id
            }
        });
        return id;
    }

    public update(entity: StreetUpdateEntity): void {
        const previousEntity = this.findById(entity.Id);
        this.dao.update(entity);
        this.triggerEvent({
            operation: "update",
            table: "STREET",
            entity: entity,
            previousEntity: previousEntity,
            key: {
                name: "Id",
                column: "STREET_ID",
                value: entity.Id
            }
        });
    }

    public upsert(entity: StreetCreateEntity | StreetUpdateEntity): number {
        const id = (entity as StreetUpdateEntity).Id;
        if (!id) {
            return this.create(entity);
        }

        const existingEntity = this.findById(id);
        if (existingEntity) {
            this.update(entity as StreetUpdateEntity);
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
            table: "STREET",
            entity: entity,
            key: {
                name: "Id",
                column: "STREET_ID",
                value: id
            }
        });
    }

    public count(options?: StreetEntityOptions): number {
        return this.dao.count(options);
    }

    public customDataCount(): number {
        const resultSet = query.execute('SELECT COUNT(*) AS COUNT FROM "STREET"');
        if (resultSet !== null && resultSet[0] !== null) {
            if (resultSet[0].COUNT !== undefined && resultSet[0].COUNT !== null) {
                return resultSet[0].COUNT;
            } else if (resultSet[0].count !== undefined && resultSet[0].count !== null) {
                return resultSet[0].count;
            }
        }
        return 0;
    }

    private async triggerEvent(data: StreetEntityEvent | StreetUpdateEntityEvent) {
        const triggerExtensions = await extensions.loadExtensionModules("kinder-map-entities-Street", ["trigger"]);
        triggerExtensions.forEach(triggerExtension => {
            try {
                triggerExtension.trigger(data);
            } catch (error) {
                console.error(error);
            }            
        });
        producer.topic("kinder-map-entities-Street").send(JSON.stringify(data));
    }
}

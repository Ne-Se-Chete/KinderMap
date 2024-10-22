import { query } from "sdk/db";
import { producer } from "sdk/messaging";
import { extensions } from "sdk/extensions";
import { dao as daoApi } from "sdk/db";

export interface AddressEntity {
    readonly Id: number;
    StreetNumber?: number;
    Street?: number;
}

export interface AddressCreateEntity {
    readonly StreetNumber?: number;
    readonly Street?: number;
}

export interface AddressUpdateEntity extends AddressCreateEntity {
    readonly Id: number;
}

export interface AddressEntityOptions {
    $filter?: {
        equals?: {
            Id?: number | number[];
            StreetNumber?: number | number[];
            Street?: number | number[];
        };
        notEquals?: {
            Id?: number | number[];
            StreetNumber?: number | number[];
            Street?: number | number[];
        };
        contains?: {
            Id?: number;
            StreetNumber?: number;
            Street?: number;
        };
        greaterThan?: {
            Id?: number;
            StreetNumber?: number;
            Street?: number;
        };
        greaterThanOrEqual?: {
            Id?: number;
            StreetNumber?: number;
            Street?: number;
        };
        lessThan?: {
            Id?: number;
            StreetNumber?: number;
            Street?: number;
        };
        lessThanOrEqual?: {
            Id?: number;
            StreetNumber?: number;
            Street?: number;
        };
    },
    $select?: (keyof AddressEntity)[],
    $sort?: string | (keyof AddressEntity)[],
    $order?: 'asc' | 'desc',
    $offset?: number,
    $limit?: number,
}

interface AddressEntityEvent {
    readonly operation: 'create' | 'update' | 'delete';
    readonly table: string;
    readonly entity: Partial<AddressEntity>;
    readonly key: {
        name: string;
        column: string;
        value: number;
    }
}

interface AddressUpdateEntityEvent extends AddressEntityEvent {
    readonly previousEntity: AddressEntity;
}

export class AddressRepository {

    private static readonly DEFINITION = {
        table: "ADDRESS",
        properties: [
            {
                name: "Id",
                column: "ADDRESS_ID",
                type: "INTEGER",
                id: true,
                autoIncrement: true,
            },
            {
                name: "StreetNumber",
                column: "ADDRESS_STREETNUMBER",
                type: "INTEGER",
            },
            {
                name: "Street",
                column: "ADDRESS_STREET",
                type: "INTEGER",
            }
        ]
    };

    private readonly dao;

    constructor(dataSource = "DefaultDB") {
        this.dao = daoApi.create(AddressRepository.DEFINITION, null, dataSource);
    }

    public findAll(options?: AddressEntityOptions): AddressEntity[] {
        return this.dao.list(options);
    }

    public findById(id: number): AddressEntity | undefined {
        const entity = this.dao.find(id);
        return entity ?? undefined;
    }

    public create(entity: AddressCreateEntity): number {
        const id = this.dao.insert(entity);
        this.triggerEvent({
            operation: "create",
            table: "ADDRESS",
            entity: entity,
            key: {
                name: "Id",
                column: "ADDRESS_ID",
                value: id
            }
        });
        return id;
    }

    public update(entity: AddressUpdateEntity): void {
        const previousEntity = this.findById(entity.Id);
        this.dao.update(entity);
        this.triggerEvent({
            operation: "update",
            table: "ADDRESS",
            entity: entity,
            previousEntity: previousEntity,
            key: {
                name: "Id",
                column: "ADDRESS_ID",
                value: entity.Id
            }
        });
    }

    public upsert(entity: AddressCreateEntity | AddressUpdateEntity): number {
        const id = (entity as AddressUpdateEntity).Id;
        if (!id) {
            return this.create(entity);
        }

        const existingEntity = this.findById(id);
        if (existingEntity) {
            this.update(entity as AddressUpdateEntity);
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
            table: "ADDRESS",
            entity: entity,
            key: {
                name: "Id",
                column: "ADDRESS_ID",
                value: id
            }
        });
    }

    public count(options?: AddressEntityOptions): number {
        return this.dao.count(options);
    }

    public customDataCount(): number {
        const resultSet = query.execute('SELECT COUNT(*) AS COUNT FROM "ADDRESS"');
        if (resultSet !== null && resultSet[0] !== null) {
            if (resultSet[0].COUNT !== undefined && resultSet[0].COUNT !== null) {
                return resultSet[0].COUNT;
            } else if (resultSet[0].count !== undefined && resultSet[0].count !== null) {
                return resultSet[0].count;
            }
        }
        return 0;
    }

    private async triggerEvent(data: AddressEntityEvent | AddressUpdateEntityEvent) {
        const triggerExtensions = await extensions.loadExtensionModules("kinder-map-entities-Address", ["trigger"]);
        triggerExtensions.forEach(triggerExtension => {
            try {
                triggerExtension.trigger(data);
            } catch (error) {
                console.error(error);
            }            
        });
        producer.topic("kinder-map-entities-Address").send(JSON.stringify(data));
    }
}

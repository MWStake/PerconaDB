<?php

/**
 * Copyright (C) 2019  NicheWork, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @author Mark A. Hershberger <mah@nichework.com>
 */
namespace MediaWiki\Extension\PerconaDB;

use Database;
use DatabaseUpdater;

class PrimaryKeyCreator {
	/** @var Database */
	protected $dbw;
	/** @var DatabaseUpdater */
	protected $upd;
	/** @var string */
	protected $dbName;
	/** @var string */
	protected $quotedDBName;
	/** @var string */
	protected $idQuotedDBName;
	/** @var string[][][] */
	protected $tableMap;

	/**
	 * @param DatabaseUpdater $upd
	 */
	public function __construct( DatabaseUpdater $upd ) {
		$this->upd = $upd;
		$this->dbw = $upd->getDB();
		$this->dbName = $this->dbw->getDBName();
		$this->quotedDBName = $this->dbw->addQuotes( $this->dbName );
		$this->idQuotedDBName = $this->dbw->addIdentifierQuotes( $this->dbName );
		$this->tableMap = [];
	}

	/**
	 * Take care of everything that needs to be done to give all the tables a primary key.
	 */
	public function execute(): void {
		if ( $this->hasTablesWithoutPrimaryKeys() ) {
			$this->addPrimaryKeys();
		}
	}

	/**
	 * Add primary keys to tables that need them.
	 */
	protected function addPrimaryKeys(): void {
		$this->upd->output( "Updating tables to have a primary key...\n" );
		foreach ( array_keys( $this->tableMap ) as $table ) {
			$this->upd->output( "...$table\n" );
			$this->addPrimaryKey( $table );
		}
	}

	/**
	 * Store an array of tables without a primary key and the type of key
	 * (UNI, MUL) used for each column.  This information is used later to
	 * construct a primary key. Returns true if any are found.
	 * @return bool
	 */
	protected function hasTablesWithoutPrimaryKeys(): bool {
		$res = $this->dbw->query(
			"SELECT DISTINCT table_name, column_key, column_name
						FROM information_schema.columns c1
					   WHERE column_key IN ('MUL', 'UNI')
						 AND table_schema={$this->quotedDBName}
						 AND NOT EXISTS (SELECT *
										   FROM information_schema.columns
										  WHERE c1.table_name = table_name
											AND column_key = 'PRI'
											AND table_schema={$this->quotedDBName})"
		);
		if ( $res->numRows() ) {
			foreach ( $res as $row ) {
				$this->tableMap[$row->table_name][$row->column_key][] = $row->column_name;
			}
		}
		return count( $this->tableMap ) > 0;
	}

	/**
	 * Add a primary key to the table that doesn't currently have one.
	 * @param string $table
	 */
	protected function addPrimaryKey( string $table ): void {
		$sql = $this->getSQLToAddPrimaryKey( $table );
		if ( $sql === false ) {
			throw new Exception( "Could not create a primary key for the $table.\n" );
		}
		$this->dbw->query( $sql );
	}

	public static function getSMWIndexMap(): array {
		return [
			'smw_di_time'     => 'p_id,s_id,o_serialized',
			'smw_di_blob'     => 'p_id,s_id,o_hash',
			'smw_di_bool'     => 'p_id,s_id,o_value',
			'smw_di_uri'      => 'p_id,s_id,o_serialized',
			'smw_di_coords'   => 'p_id,s_id,o_serialized',
			'smw_di_number'   => 'p_id,s_id,o_serialized',
			'smw_fpt_ask'     => 's_id,o_id',
			'smw_concept_cache' => 's_id,o_id',
			'smw_fpt_askpa'	  => 's_id,o_hash',
			'smw_fpt_askde'   => 's_id,o_serialized',
			'smw_fpt_askfo'   => 's_id,o_hash',
			'smw_fpt_askdu'   => 's_id,o_serialized',
			'smw_fpt_asksi'   => 's_id,o_serialized',
			'smw_fpt_askst'   => 's_id,o_hash',
			'smw_fpt_cdat'    => 's_id,o_serialized',
			'smw_fpt_conc'    => 's_id',
			'smw_fpt_conv'    => 's_id,o_hash',
			'smw_fpt_dtitle'  => 's_id,o_hash',
			'smw_fpt_impo'    => 's_id,o_hash',
			'smw_fpt_inst'    => 's_id,o_id',
			'smw_fpt_lcode'   => 's_id,o_hash',
			'smw_fpt_ledt'    => 's_id,o_id',
			'smw_fpt_list'    => 's_id,o_hash',
			'smw_fpt_mdat'    => 's_id,o_serialized',
			'smw_fpt_media'   => 's_id,o_hash',
			'smw_fpt_mime'    => 's_id,o_hash',
			'smw_fpt_newp'    => 's_id,o_value',
			'smw_fpt_pplb'    => 's_id,o_id',
			'smw_fpt_prec'    => 's_id,o_serialized',
			'smw_fpt_pval'    => 's_id,o_hash',
			'smw_fpt_redi'    => 's_title,s_namespace',
			'smw_fpt_serv'    => 's_id,o_hash',
			'smw_fpt_sobj'    => 's_id,o_id',
			'smw_fpt_subc'    => 's_id,o_id',
			'smw_fpt_subp'    => 's_id,o_id',
			'smw_fpt_text'    => 's_id,o_hash',
			'smw_fpt_type'    => 's_id,o_serialized',
			'smw_fpt_unit'    => 's_id,o_hash',
			'smw_fpt_uri'     => 's_id,o_serialized',
			'smw_query_links' => 's_id,o_id'
		];
	}

	protected function createSQLMap( string $table, string $fields ) {
		return $this->createSQL( $table, [
			'index' => '(' . $fields . ')',
			'column' => explode( ",", $fields )
		] );
	}

	/**
	 * Get the SQL to add a primary key.
	 * @param string $table
	 * @return string
	 */
	protected function getSQLToAddPrimaryKey( string $table ): string {
		global $wgDBprefix;
		$sql = false;
		$prefLen = strlen( $wgDBprefix );
		$origTable = $table;

		if ( $prefLen > 0 && substr( $table, 0, $prefLen ) === $wgDBprefix ) {
			$table = substr( $table, $prefLen );
		}

		$sqlMap = array_merge(
			self::getSMWIndexMap(),
			[
				'oldimage' => [
					'index' => '( oi_sha1, oi_timestamp )',
					'column' => [ 'oi_sha1', 'oi_timestamp' ]
				],
				'querycache' => [
					'index' => '( qc_namespace, qc_title )',
					'column' => [ 'qc_namespace', 'qc_title' ]
				],
				'querycachetwo' => [
					'ALTER TABLE querycachetwo '
					. 'ADD COLUMN qcc_id int(10) UNSIGNED NOT NULL '
					. 'AUTO_INCREMENT PRIMARY KEY'
				],
				'user_newtalk' => [
					'index' => '( user_id, user_ip, user_last_timestamp )',
					'column' => [ 'user_id', 'user_ip', 'user_last_timestamp' ]
				]
			]
		);
		if ( isset( $sqlMap[$table] ) ) {
			if (
				isset( $sqlMap[$table]['index'] )
				&& isset( $sqlMap[$table]['column'] )
			) {
				$sql = $this->createSQL( $origTable, $sqlMap[$table] );
			} elseif (
				isset( $sqlMap[$table] ) && is_array( $sqlMap[$table] )
				&& count( $sqlMap[$table] ) === 1
			) {
				$sql = array_shift( $sqlMap[$table] );
			} elseif (
				isset( $sqlMap[$table] ) && is_string( $sqlMap[$table] )
			) {
				$sql = $this->createSQLMap( $origTable, $sqlMap[$table] );
			} else {
				$this->upd->output( "Entry for '$table' is not correctly formed!" );
				return false;
			}
		}
		if ( $sql ) {
			$sql .= " COMMENT 'added by PrimaryKeyCreator'";
		}
		return $sql;
	}

	/**
	 * Produce an SQL query while ensuring there are no duplicates
	 * @param string $table
	 * @param string[] $map
	 * @return string
	 */
	protected function createSQL( string $table, array $map ) {
		$this->upd->output(
			"Verify that $table has no dupes for the the index: "
			. $map['index'] . "...\n"
		);

		# create temporary table with counts for each duplicate columns
		$columns = implode( ",", $map['column'] );
		$tableName = $this->dbw->addIdentifierQuotes( "temp$table" );
		$sql = <<<EOB
CREATE TEMPORARY TABLE $tableName (
     SELECT count(*) AS _count, $columns
       FROM $table
   GROUP BY $columns
);
EOB;
		$this->dbw->query( $sql );
		$res = $this->dbw->query( "SELECT * FROM $tableName WHERE _count > 1 ORDER BY _count DESC" );
		foreach ( $res as $row ) {
			$dupe = array_map(
				function ( $col ) use ( $row ) {
					return $this->dbw->addIdentifierQuotes( $col )
						. " = " . $this->dbw->addQuotes( $row->$col );
				},
				array_filter(
					array_keys( get_object_vars( $row ) ),
					static function ( $key ) {
						return !is_int( $key ) && $key !== '_count';
					}
				)
			);
			$this->upd->output( "... Removing " . implode( ', ', $dupe ) . "\n" );
			// TODO get and store current state
			$this->dbw->query( "SET GLOBAL pxc_strict_mode=PERMISSIVE" );
			$this->dbw->query( "DELETE FROM $table WHERE " . implode( " AND ", $dupe ) );
			$this->dbw->query( "SET GLOBAL pxc_strict_mode=ENFORCING" );
		}
		$this->dbw->query( "DROP TABLE $tableName" );
		return sprintf(
			"ALTER TABLE %s ADD PRIMARY KEY %s",
			$this->dbw->addIdentifierQuotes( $table ), $map['index']
		);
	}
}
